(ns
  ^{:doc "All functions are concerned with retreiving metadata and offsets from kafka brokers
          part of this task requires blacklisting of brokers that do not response and retrying on connection errors"}
  kafka-clj.metadata
  (:require
    [clj-tuple :refer [tuple]]
    [kafka-clj.produce :refer [metadata-request-producer send-metadata-request shutdown]]
    [fun-utils.core :refer [fixdelay]]
    [clojure.tools.logging :refer [info error warn]]
    [clojure.core.async :refer [go <! <!! >!! alts!! timeout thread]]
    [kafka-clj.produce :as produce])
  (:import (clojure.lang IDeref)))

(declare get-metadata-recreate!)

(defn- convert-metadata-response [resp]
  ;; transform the resp into a map
  ;; {topic-name [{:host host :port port :isr [ {:host :port}] :error-code code} ] }
  ;; the index of the vector (value of the topic-name) is sorted by partition number 
  ;; here topic-name:String and partition-n:Integer are keys but not keywords
  ;;{:correlation-id 2,
	;;											 :brokers [{:node-id 0, :host a, :port 9092}],
	;;											 :topics
	;;											 [{:error-code 10,
	;;											   :topic p,
	;;											   :partitions
	;;											   [{:partition-error-code 10,
	;;											     :partition-id 0,
	;;											     :leader 0,
	;;											     :replicas '(0 1),
	;;											     :isr '(0)}]}]}"
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

(defn send-update-metadata [producer conf]
  (send-metadata-request producer conf))

(defn get-broker-metadata [metadata-producer {:keys [metadata-timeout] :or {metadata-timeout 5000} :as conf}]
   "
   Creates a metadata-request-producer, sends a metadata request to the broker and waits for a result,
   if no result in $metadata-timeout or an error an exception is thrown, otherwise the result of
   (convert-metadata-response resp) is returned.
   "
   (let [producer metadata-producer
         read-ch  (-> producer :client :read-ch)
         error-ch (-> producer :client :error-ch)]
	      (send-update-metadata producer conf)
	          ;wait for response or timeout
	          (let [[v c] (alts!! [read-ch error-ch (timeout metadata-timeout)])]
	             (if v
	               (if (= c read-ch)  (convert-metadata-response v)
	                 (throw (Exception. (str "Error reading metadata from producer " metadata-producer  " error " v))))
	               (do
                   (shutdown producer)
                   (throw (Exception. (str "timeout reading from producer " (vals metadata-producer)))))))))

(defn blacklisted?
  "
  blacklisted-metadata-producers a map of {{:host <host> :port <port>} <bool>}} here bool doesn't really matter, if listed an item is black listed
  host the host name
  port the port number
  Returns nil if the host port is not found in the blacklisted-metadata-producers"
  [blacklisted-metadata-producers host port]
  {:pre [(associative? blacklisted-metadata-producers) host port]}
  (get blacklisted-metadata-producers {:host host :port port}))

(defn- is-blacklisted?
  [blacklisted-producers [k _]]
  (get blacklisted-producers k))

(defn black-list-producer!
  "
  Runs commute to add the host and port to the blacklisted-metadata-producers-ref
  blacklisted-metadata-producers-ref a ref that contains the map {{:host <host> :port <port>} ... }
  {:keys [host port]} host the host name, port the port number
  e the exception that produced the error, may be nil
  "
  [blacklisted-metadata-producers-ref {:keys [host port]} e]
  {:pre [blacklisted-metadata-producers-ref host (number? port)]}
  (error e (str "Blacklisting metadata-producer: " host ":" port))
  (dosync (commute blacklisted-metadata-producers-ref assoc {:host host :port port} true))
  nil)

(defn- un-blacklist-producer!
  "Remove the {:host :port} combindation from the blacklisted-metadata-producers-ref"
  [blacklisted-metadata-producers-ref {:keys [host port]}]
  {:pre [host (number? port)]}
  (info "Unblacklist metadata producer: " host ":" port)
  (dosync
    (alter blacklisted-metadata-producers-ref dissoc {:host host :port port})))

(defn- client-closed? [producer]
  (produce/producer-closed? producer))

(defn- blacklist-if-exception
  "On exception will blacklist the producer the return nil, otherwise returns what f returns"
  [blacklisted-metadata-producers-ref k f & args]
  (try
    (apply f args)
    (catch Exception e (do
                         (error "ERROR Blacklisting")
                         (black-list-producer! blacklisted-metadata-producers-ref k e)
                         nil))))

(defn- recreate-producer-if-closed! [metadata-producers-ref conf k metadata-producer-delay]
  (let [metadata-producer @metadata-producer-delay]
    (if (client-closed? metadata-producer)
      (do
        (let [producer (metadata-request-producer (:host metadata-producer) (:port metadata-producer) conf)]
          (dosync (alter metadata-producers-ref assoc k (delay producer)))
          producer))
      metadata-producer)))

(defn- _get-meta! [metadata-producers-ref conf k producer-delay]
  (when-let [metadata-producer (recreate-producer-if-closed! metadata-producers-ref conf k producer-delay)]
    (get-broker-metadata metadata-producer conf)))

(defn unchunk [s]
  (when (seq s)
    (lazy-seq
      (cons (first s)
            (unchunk (next s))))))

(defn iterate-metadata-producers! [metadata-producers-ref blacklisted-metadata-producers-ref conf]
  {:pre [metadata-producers-ref blacklisted-metadata-producers-ref]}
  (->> @metadata-producers-ref
       (filter (complement (partial is-blacklisted? @blacklisted-metadata-producers-ref)))
       shuffle
       unchunk
       (map (fn [[k producer-delay]] (blacklist-if-exception blacklisted-metadata-producers-ref k #(_get-meta! metadata-producers-ref conf k producer-delay))))
       (filter (complement nil?))
       first))

(defn- ref? [r]
  (when r
    (instance? IDeref r)))

(defn get-offsets! [{:keys [metadata-producers-ref blacklisted-metadata-producers-ref]} conf]
  )

(defn get-metadata! [{:keys [metadata-producers-ref blacklisted-metadata-producers-ref]} conf]
  {:pre [(ref? metadata-producers-ref) (ref? blacklisted-metadata-producers-ref)]}
  (error "trying to get metadata:")
  (iterate-metadata-producers! metadata-producers-ref blacklisted-metadata-producers-ref conf))


(defn get-metadata [metadata-producers conf]
  (get-metadata! {:metadata-producers-ref (ref (reduce (fn [m {:keys [host port] :as producer}] (assoc m {:host host :port port} (delay producer))) {} metadata-producers))
                  :blacklisted-metadata-producers-ref (ref {})}
    conf))

(defn try-unblacklist!
  "Checks all the blacklisted connections and if a reconnect is possible, the connection is moved from teh black-listed-metadata-producers-ref to metadata-producer-"
  [metadata-producers-ref blacklisted-metadata-producers-ref conf]
  (doseq [metadata-producer (->> @metadata-producers-ref
                                 (filter (partial is-blacklisted? @blacklisted-metadata-producers-ref))
                                 (map (fn [[k producer-delay]] (try (recreate-producer-if-closed! metadata-producers-ref conf k producer-delay) (catch Exception _ nil))))
                                 (filter (complement nil?)))]
    (un-blacklist-producer! blacklisted-metadata-producers-ref metadata-producer)))

(defn bootstrap->producermap
  "Takes bootstrap-brokers = [{:host :port} ...] and returns a map {host:port (delay metadata-producer}}"
  [bootstrap-brokers conf]
  (reduce (fn [state {:keys [host port]}]  (assoc state {:host host :port port} (delay (metadata-request-producer host port conf)))) {} bootstrap-brokers))
