(ns
  ^{:doc "All functions are concerned with retreiving metadata and offsets from kafka brokers
          part of this task requires blacklisting of brokers that do not response and retrying on connection errors"}
  kafka-clj.metadata
  (:require
    [clj-tuple :refer [tuple]]
    [kafka-clj.buff-utils :refer [write-short-string with-size compression-code-mask]]
    [kafka-clj.response :as kafka-resp]
    [clojure.tools.logging :refer [info error warn debug]]
    [kafka-clj.protocol :as protocol]
    [tcp-driver.driver :as tcp-driver]
    [tcp-driver.io.pool :as tcp-pool]
    [tcp-driver.io.conn :as tcp-conn]

    [tcp-driver.io.stream :as tcp-stream]

    [kafka-clj.tcp :as tcp]
    [tcp-driver.io.stream :as driver-io]
    [schema.core :as s])
  (:import
    (io.netty.buffer Unpooled ByteBuf)
    (kafka_clj.util Util)))

;;validates metadata responses like {"abc" [{:host "localhost", :port 50738, :isr [{:host "localhost", :port 50738}], :id 0, :error-code 0}]}
(def META-RESP-SCHEMA {s/Str [{:host s/Str, :port s/Int, :isr [{:host s/Str, :port s/Int}], :id s/Int, :error-code s/Int}]})

(defn- convert-metadata-response
  "
    transform the resp into a map
     {topic-name [{:host host :port port :isr [ {:host :port}] :error-code code} ] }
     the index of the vector (value of the topic-name) is sorted by partition number
     here topic-name:String and partition-n:Integer are keys but not keywords
    {:correlation-id 2,
                           :brokers [{:node-id 0, :host a, :port 9092}],
                           :topics
                           [{:error-code 10,
                             :topic p,
                             :partitions
                             [{:partition-error-code 10,
                               :partition-id 0,
                               :leader 0,
                               :replicas '(0 1),
                               :isr '(0)}]}]}

   "
    [resp]

    (let [m (let [;convert the brokers to a map {:broker-node-id {:host host :port port}}
                  brokers-by-node (into {} (map (fn [{:keys [node-id host port]}] [ node-id {:host host :port port}]) (:brokers resp)))]
                  ;convert the response message to a map {topic-name {partition {:host host :port port}}}
                  (into {}
                         (for [topic (:topics resp) :when (= (:error-code topic) 0)]
                              [(:topic topic) (apply tuple (vals (apply sorted-map (flatten
                                                                                      (for [partition (:partitions topic)
                                                                                             :let [broker (get brokers-by-node (:leader partition))
                                                                                                   isr (mapv #(get brokers-by-node %) (:isr partition))]]
                                                                                         [(:partition-id partition)
                                                                                          {:host (:host broker)
                                                                                           :port (:port broker)
                                                                                           :isr  isr
                                                                                           :id (:partition-id partition)
                                                                                           :error-code (:partition-error-code partition)}])))))])))]
      m))



(defn write-metadata-request
  "
   see http://kafka.apache.org/protocol.html
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    ApiKey => int16
    ApiVersion => int16
    CorrelationId => int32
    ClientId => string
    MetadataRequest => [TopicName]
       TopicName => string
   "
  [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"}}]
  (-> buff
      (.writeShort (short protocol/API_KEY_METADATA_REQUEST))        ;api-key
      (.writeShort (short protocol/API_VERSION))                     ;version api
      (.writeInt (int correlation-id))                      ;correlation id
      (write-short-string client-id)                        ;short + client-id bytes
      (.writeInt (int 0))))                                 ;write empty topic, dont use -1 (this means nil), list to receive metadata on all topics


(defn send-recv-metadata-request
  "Writes out a metadata request to the producer con"
  [conn conf]
  (let [timeout 30000

        ^ByteBuf buff (Unpooled/buffer)
        _ (with-size buff write-metadata-request conf)

        ^"[B" arr (Util/toBytes buff)

        _  (driver-io/write-bytes conn arr 0 (count arr))

        _ (driver-io/flush-out conn)

        resp-len (driver-io/read-int conn timeout)

        resp-buff (Unpooled/wrappedBuffer
                    ^"[B" (driver-io/read-bytes conn resp-len timeout))

        resp (kafka-resp/read-metadata-response ^ByteBuf resp-buff)

        converted-resp (convert-metadata-response
                         resp)

        check-host-meta (fn [topic {:keys [host error-code] :as host-meta}]
                          (if (and host
                                   (number? error-code)
                                   (zero? error-code))
                            host-meta
                            (let [error-msg  (str "Excluding broken host metadata for " topic " => " host-meta)]
                              (error error-msg)
                              nil)))]

        ;;The aim is to exclude any nil host entries or erorr_code > 0, an error message is printed on exclude
        (reduce-kv (fn [state log hosts-meta]
                     (assoc state log (filterv (partial check-host-meta log) hosts-meta)))
                   {}
                   converted-resp)))

  (defn safe-nth [coll i]
    (let [v (vec coll)]
      (when (> (count v) i)
        (nth v i))))

  (defn get-cached-metadata
    "from the metadata connector, reads the metadata in the metadata-ref

     Just calling metadata without a partition returns ;;[{:host \"localhost\", :port 50738, :isr [{:host \"localhost\", :port 50738}]
     specifying a partition returns the corresponding entry"
    ([metadata-connector topic partition]
      {:pre [(number? partition)]}
     (safe-nth (get-cached-metadata metadata-connector topic)
               partition))

    ([metadata-connector topic]
     {:pre [metadata-connector (or (string? topic) (keyword? topic))]}

     (let [metadata @(:metadata-ref metadata-connector)]

       (get metadata topic))))

(defn get-cached-brokers
  "Return the current registered brokers in the metadata cache"
  ([metadata-connector]
   @(:brokers-ref metadata-connector))

  ([metadata-connector topic]
   {:pre [metadata-connector (string? topic)]}

   (let [metadata @(:metadata-ref metadata-connector)]

     (get metadata topic))))

  (defn get-metadata
    "
    Return metadata in the form ;;validates metadata responses like {\"abc\" [{:host \"localhost\", :port 50738, :isr [{:host \"localhost\", :port 50738}], :id 0, :error-code 0}]}\n
    "
    [{:keys [driver]} conf]
    (:pre [driver])
    (tcp-driver/send-f driver
                       (fn [conn]
                         (locking conn
                           (try
                             (send-recv-metadata-request conn conf)
                             (catch Exception e (do
                                                  (error "Error retreiving metadata " e)
                                                  (throw e))))))
                       10000))

;;;;validates metadata responses like {"abc" [{:host "localhost", :port 50738, :isr [{:host "localhost", :port 50738}], :id 0, :error-code 0}]}

(defn update-metadata!
  "Updates the borkers-ref, metadata-ref and add/remove hosts as per the metadata from the driver
        if meta is nil, any updates are skipped
   "
  [{:keys [brokers-ref metadata-ref] :as connector} conf]

    (when-let [meta (get-metadata connector conf)]


      (let [select-host #(select-keys % [:host :port])

            hosts (->>                                      ;;extract the host and isr hosts into a set from the meta data
                    meta
                    vals
                    flatten
                    (mapcat #(conj (map select-host (:isr %))
                                   (select-host %)))

                    (filter #(every? (complement nil?) (vals %)))

                    (into #{}))

            hosts-remove (clojure.set/difference @brokers-ref hosts)
            hosts-add (clojure.set/difference hosts @brokers-ref)]

        (doseq [host hosts-add]
          (tcp-driver/add-host (:driver connector) host))

        ;;TODO we cannot use remove if it means having no more hosts
        ;; solution would be to remove only non bootstrap broker nodes
        ;(doseq [host hosts-remove]
        ;  (tcp-driver/remove-host (:driver connector) host))

        (dosync
          (commute
            brokers-ref #(into #{} (concat hosts %)))
          (commute
            metadata-ref (constantly meta)))

        meta)))

  (defn blacklisted? [{:keys [driver] :as st} host-address]
    {:pre [driver (:host host-address) (:port host-address)]}
    (info "blacklisted from " (keys st))
    (tcp-driver/blacklisted? driver host-address))

  (defn connector
    "Creates a tcp pool and initiate for all brokers"
  [bootstrap-brokers conf]
  (let [driver (tcp/driver bootstrap-brokers :retry-limit (get conf :retry-limit 3) :pool-conf conf)
        brokers (into #{} bootstrap-brokers)]

    {:conf conf
     :driver driver
     :metadata-ref (ref nil)
     :brokers-ref (ref brokers)}))

(defn close [{:keys [driver]}]
  {:pre [driver]}
  (tcp-driver/close driver))
