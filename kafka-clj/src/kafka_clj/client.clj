(ns kafka-clj.client
  (:require
    [fun-utils.core :as futils]
    [fun-utils.threads :as fthreads]
    [fun-utils.cache :as cache]
    [kafka-clj.produce :as produce]
    [kafka-clj.buff-utils :refer [write-short-string with-size compression-code-mask]]
    [kafka-clj.response :as kafka-resp]
    [kafka-clj.tcp :as tcp]
    [kafka-clj.metadata :as meta]
    [kafka-clj.msg-persist :as persist]
    [kafka-clj.topics :as topics]
    [clojure.tools.logging :refer [error info debug warn]]
    [com.stuartsierra.component :as component]
    [clj-tuple :refer [tuple]]
    [clojure.core.async :as async]
    [kafka-clj.metadata :as kafka-metadata]
    [tcp-driver.io.stream :as tcp-stream]
    [tcp-driver.driver :as tcp-driver]
    [schema.core :as s])
  (:import [java.util.concurrent.atomic AtomicLong AtomicBoolean]
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit ThreadFactory ExecutorService CountDownLatch)
           (java.io ByteArrayInputStream DataInputStream)
           (io.netty.buffer ByteBuf Unpooled)
           (kafka_clj.util Util)
           (java.util Map)))

(defrecord ErrorCtx [code topic msgs])
(defrecord ProducerState [conf
                          ^AtomicLong activity-counter
                          send-cache
                          retry-cahe
                          metadata-connector
                          ;send-connector
                          producer-error-ch])

(declare handle-async-topic-messages)

(defn- ^ThreadFactory daemon-thread-factory []
  (reify ThreadFactory
    (newThread [_ r]
      (doto (Thread. ^Runnable r) (.setDaemon true)))))

(defn- dissoc-in
  [m [k & ks]]
  (if-not ks
    (dissoc m k)
    (assoc m k (dissoc-in (m k) ks))))

(defn- no-error-status [{:keys [error-code]}]
  (if (number? error-code)
    (let [error-code' (int error-code)]
      (not
        (or
          (= error-code' 5)
          (= error-code' 9))))
    true))

(defn- healthy-partition-rc? [{:keys [metadata-connector] :as state} node-data]
  (try
    ;we can have host and port nil here if the connection failed via an exception condition
    ;(info "healthy-partition-rc? is-black-listed; " (kafka-metadata/blacklisted? metadata-connector (select-keys node-data [:host :port])) " host " (select-keys node-data [:host :port]))
    (and
      (:host node-data)
      (:port node-data)
      (no-error-status (:error-code node-data)))
      (not (kafka-metadata/blacklisted? metadata-connector (select-keys node-data [:host :port])))
    (catch Exception e (do
                         (error (str "Error while querying keys " (keys state) " data " node-data) e)
                         false))))

(defn create-topic-request
  "Send a create topic request to a random broker and return true,
   otherwise throw an exception
   connector == kafka-clj.metadata/connector
   "
  [{:keys [metadata-connector]} topic]

  {:pre [metadata-connector (:conf metadata-connector)]}

  (let [conf (:conf metadata-connector)
        resp (tcp-driver/select-send! (:driver metadata-connector)
                                      (fn [conn]
                                        (first (topics/create-topics conn
                                                                     [{:topic              topic
                                                                       :partitions         (:default-partitions conf 1)
                                                                       :replication-factor (:default-replication-factor conf 1)}]
                                                                     10000)))
                                      30000)]

    (prn "create-topic-request resp " resp)
    (when-not (#{0 36} (:error_code resp))
      (throw (RuntimeException. (str "Failed to create topic " topic " response " resp))))))

(defn try-create-topic
  "Return true if create-topic was called, otherwise an exception is thrown"
  [{:keys [state] :as connector} topic]
  (let [topic-auto-create (get-in state [:conf :topic-auto-create])]

    (prn "try-create-topic : topic-auto-create: " topic-auto-create)

    (if topic-auto-create
      (create-topic-request connector topic)
      (throw (RuntimeException. (str "No healthy brokers available for topic " topic " " connector))))))

(defn send-msg
  "Sends a message async and returns false if the connection is closed
  Any errors uncrecoverable errors shuold be readh from the producer-error-ch"
  [{:keys [state metadata-connector msg-ch] :as connector} topic ^bytes bts]

  (loop [retry 0]
    (if (and (-> state :conf :batch-fail-message-over-limit) (>= (count bts) ^long (-> state :conf :batch-byte-limit)))
      (throw (RuntimeException. (str "The message size [ " (count bts) " ] is larger than the configured batch-byte-limit [ " (get-in state [:conf :batch-byte-limit]) "]")))

      (if (first
            (filter
              (partial healthy-partition-rc? connector)
              (kafka-metadata/get-cached-metadata metadata-connector topic)))
        ;;checkin for boolean false here, aparently some code generated by clojure async in some versions and situations
        ;;always return nil so it is not matter if its closed or not nil is returned, later versions return TRUE
        (when (= (async/>!! msg-ch (produce/message topic -1 bts)) Boolean/FALSE)
          (throw (RuntimeException. "The client has been shutdown")))

        (if (pos? retry)
          (throw (RuntimeException. (str "No healthy brokers available for topic " topic " " connector)))
          (do
            (kafka-metadata/update-metadata! metadata-connector (:conf state))
            (when (empty? (kafka-metadata/get-cached-metadata metadata-connector topic))
              (try-create-topic connector topic)
              (let [new-meta (kafka-metadata/update-metadata! metadata-connector (:conf state))]
                (debug "New metadata after try-create-topic: " new-meta)))

            (recur (inc retry))))))))

(defn close
  "Close all producers and channels created for the connected"

  [{{:keys [^AtomicBoolean shutdown-flag
            ^AtomicLong activity-counter
            metadata-connector
            scheduled-service
            retry-cache]} :state
    msg-ch                :msg-ch
    persist-delay-thread  :persist-delay-thread
    async-latch           :async-latch}]

  ;;set shutdown flag
  (.set shutdown-flag true)

  (async/close! msg-ch)

  ;only close when all activity has ceased
  (loop [v (.get activity-counter)]
    (Thread/sleep 500)
    (if (not= v (.get activity-counter))
      (recur (.get activity-counter))))

  (.shutdownNow ^ScheduledExecutorService scheduled-service)
  (kafka-metadata/close metadata-connector)
  ;(kafka-metadata/close send-connector)

  ;;before we close the retry-cache we need to close
  ;; persist-delay-thread and wait for async-latch
  (async/close! persist-delay-thread)
  (.await ^CountDownLatch async-latch 5000 TimeUnit/MILLISECONDS)

  (persist/close-retry-cache retry-cache))

(defn- retry-msg-bts-seq [retry-msg]
  (let [msg (:v retry-msg)]
    (if (or (record? msg) (map? msg))
      [(:bts msg)]
      (map :bts msg))))

;;public function do not remove
(defn producer-error-ch [connector]
  (get-in connector [:state :producer-error-ch]))

;;public function do not remove
(defn get-metadata-error-ch [connector]
  (:metadata-error-ch connector))

(defn- ^ProducerState unrecoverable-error
  "At this point we can only notify the producer-error-ch and save the messages to disk"
  [^ProducerState producer-state {:keys [code topic msgs] :as error-ctx}]
  (error "unrecoverable error " code " for topic " topic " messages " (count msgs) " saving to retry cache and retry at a later time")
  (try
    (persist/write-to-retry-cache producer-state topic msgs)
    (catch Exception e (error e (str "Fatal error, cannot persist messages for topic " topic " to retry cache messages " (count msgs)))))
  (async/>!! (:producer-error-ch producer-state) error-ctx)

  producer-state)

(defn- ^ProducerState error-no-partition
  [state topic msgs]
  (unrecoverable-error state (->ErrorCtx :no-partitions topic msgs)))

(defn- rand-nth' [v] (when-not (empty? v) (rand-nth v)))

(defn select-partition-rc
  "Returns a partition-rc {:host :port} that has not been blacklisted otherwise nil"
  [{:keys [metadata-connector]} topic]
  (rand-nth'
    (filterv (complement (partial kafka-metadata/blacklisted? metadata-connector))
             (kafka-metadata/get-cached-metadata
               metadata-connector
               topic))))

(defn kafka-response
  "Handles the response input stream for each producer.
   Takes the state and resp:ProduceResponse.
   If the error-code > 0 and a send-cache is in state, the messages are retrieved from the persist and if still in persist,
   the messages are sent using handle-async-topic-messages."
  [^ProducerState state send-cache resp]
  (let [error-code (:error-code resp)                       ;;dont use {:keys []} here some weirdness was seend that caused
        topic (:topic resp)                                 ;;java.lang.IllegalArgumentException: No value supplied for key: kafka_clj.response.ProduceResponse
        partition (:partition resp)                         ;;unrolling creates a hashmap and for some reason doesn't get the supplied keys
        correlation-id (:correlation-id resp)]

    (if (and (number? error-code) topic partition)
      (when (> (long error-code) 0)
        (if send-cache
          (when-let [msg (persist/get-sent-message {:send-cache send-cache} topic partition correlation-id)]
            (handle-async-topic-messages state topic (if (coll? msg) msg [msg])))
          (error (str "Message received event though acks < 1 msg " resp))))
      (error (str "Message cannot contain nil values topic " topic " partition " partition " error-code " error-code " resp: " resp " keys " (keys resp))))))


(defn read-response [^ProducerState state send-cache conn]
  (try
    (let [len (tcp-stream/read-int conn 1000)
          bts (tcp-stream/read-bytes conn len 10000)
          resp (kafka-resp/in->kafkarespseq (DataInputStream. (ByteArrayInputStream. ^"[B" bts)))]
      (kafka-response state send-cache resp))
    (catch Exception e (error (str "Error while waiting for ack response from " conn) e))))

;this is only used with the single message producer api where ack > 0
(def global-message-ack-cache (delay (kafka-clj.msg-persist/create-send-cache {})))

(defn- ^ByteBuf unpooled-buff []
  (Unpooled/buffer))

(defn- ^ByteBuf write-request [conf message-ack-cache msgs acks]
  (let [^ByteBuf buff (unpooled-buff)]
    (with-size buff produce/write-request conf msgs)
    ;(if (pos? acks)
    ;  (produce/write-message-for-ack message-ack-cache conf msgs buff)
    ;  (with-size buff produce/write-request conf msgs))
    ;
    buff))

(defn- send-rcv-messages
  "Send messages by writing them to the tcp client.
  The write is async.
  If the conf properties acks is > 0 the messages will also be written to an inmemory cache,
  the cache expires and has a maximum size, but it allows us to retry failed messages."

  ([^ProducerState state host-address msgs]
   (send-rcv-messages state host-address @global-message-ack-cache msgs))

  ([{:keys [metadata-connector] :as state}
    host-address
    message-ack-cache
    msgs]
   {:pre [metadata-connector
          host-address
          (or (nil? message-ack-cache) (instance? Map message-ack-cache))
          msgs]}

   (let [conf (:conf state)
         acks (get conf :acks 0)

         ^ByteBuf byte-buff (write-request conf message-ack-cache msgs acks)]

     (tcp-driver/send-f (:driver metadata-connector)
                        host-address
                        (fn [conn]
                          (locking conn
                            (tcp-stream/write-bytes conn (Util/toBytes byte-buff))

                            (tcp-stream/flush-out conn)

                            ))
                        5000))))

(defn- send-data [state partition-rc _ msgs]
  (try
    (.incrementAndGet ^AtomicLong (:activity-counter state))
    (send-rcv-messages state partition-rc msgs)
    (catch Throwable t (do
                         (.printStackTrace t)
                         (throw t)))))


(defn handle-async-topic-messages
  "Send messages from the same topic to a producer"
  [^ProducerState state topic msgs]
  (try

    (if-let [partition-rc (select-partition-rc state topic)]
      (do
        (info "send-data partition-rc " partition-rc)
        (send-data state partition-rc topic (mapv #(assoc % :partition (:id partition-rc)) msgs)))
      (do
        (error-no-partition state topic msgs)))
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)
                         (clojure.core.async/>!! (:producer-error-ch state) (->ErrorCtx :error topic msgs))))))


(defn- async-handler
  "Return a thread-seq, call countdown on the latch when the async thread closes"
  [^ExecutorService exec-service ^ProducerState producer-state msg-buff ^CountDownLatch latch]

  ;Instead of async-ctx do
  ; loop on msg-buff, for each group of mssages do group-by
  ; send each separate topic to a different thread
  (futils/thread-seq2
    (fn [msgs]
      (doseq [[topic grouped-msgs] (group-by :topic msgs)]
        (fthreads/submit exec-service #(handle-async-topic-messages producer-state topic grouped-msgs))))
    (fn [] (.countDown latch))
    msg-buff))


(defn- exception-if-nil!
  "Helper function that throws an exception if the metadata passed in is nil"
  [bootstrap-brokers metadata]
  (if metadata
    metadata
    (throw (ex-info (str "No metadata could be found from any of the bootstrap brokers provided " bootstrap-brokers) {:type              :metadata-exception
                                                                                                                      :bootstrap-brokers bootstrap-brokers}))))

(defn create-connector
  "Creates a connector for sending to kafka
   All sends are asynchronous, unrecoverable errors are saved to a local retry cache and
   also notified to the producer-error-ch with data of type ErrorCtx"
  [bootstrap-brokers {:keys [acks
                             io-threads
                             batch-num-messages
                             queue-buffering-max-ms
                             default-replication-factor
                             default-partitions
                             batch-fail-message-over-limit
                             batch-byte-limit
                             topic-auto-create
                             flush-on-write]
                      :or   {batch-num-messages         25
                             queue-buffering-max-ms     500
                             io-threads                 10
                             default-replication-factor 1
                             default-partitions         2
                             acks                       0
                             batch-fail-message-over-limit true
                             batch-byte-limit 10485760
                             topic-auto-create          true
                             flush-on-write false}
                      :as   conf}]

  s/Int
  (let [^ScheduledExecutorService scheduled-service (Executors/newSingleThreadScheduledExecutor (daemon-thread-factory))
        async-ctx (fthreads/create-exec-service io-threads)
        metadata-connector (kafka-clj.metadata/connector bootstrap-brokers conf)

        ;;TODO implement acks

        ;send-connector (kafka-clj.metadata/connector bootstrap-brokers conf)

        producer-error-ch (async/chan (async/dropping-buffer 10))

        metadata-error-ch (async/chan (async/dropping-buffer 10))

        send-cache (if (> acks 0) (persist/create-send-cache conf))
        retry-cache (persist/create-retry-cache conf)

        activity-counter (AtomicLong. 0)
        async-latch (CountDownLatch. 1)

        ;;set to true on shutdown, and used to ignore NPE errors in the persist cache, and also exit the fixdelay thread
        ^AtomicBoolean shutdown-flag (AtomicBoolean. false)

        state {
               :metadata-connector metadata-connector

               :shutdown-flag      shutdown-flag
               :activity-counter   activity-counter
               :retry-cache        retry-cache
               :producer-error-ch  producer-error-ch
               :metadata-error-ch  metadata-error-ch
               :scheduled-service  scheduled-service
               :conf               (assoc conf
                                     :batch-fail-message-over-limit batch-fail-message-over-limit
                                     :batch-byte-limit batch-byte-limit
                                     :default-replication-factor default-replication-factor
                                     :default-partitions default-partitions
                                     :topic-auto-create topic-auto-create)}


        connector {:metadata-error-ch metadata-error-ch
                   :send-cache send-cache
                   :retry-cache retry-cache
                   :flush-on-write flush-on-write
                   :metadata-connector metadata-connector
                   ;:send-connector send-connector
                   :state state}

        msg-ch (async/chan 100)
        msg-buff (futils/buffered-chan msg-ch batch-num-messages queue-buffering-max-ms 5 (produce/flush-on-byte->fn batch-byte-limit))

        ;;every 5 seconds check for any data in the retry cache and resend the messages
        connector2 (assoc connector :msg-ch msg-ch)

        _ (do "calling update-metadata!: " bootstrap-brokers " conf " conf)
        update-metadata #(kafka-metadata/update-metadata! metadata-connector conf)

        persist-delay-thread (futils/fixdelay-thread 1000
                                                     (try
                                                       (when-not (or (.get shutdown-flag) (persist/closed? connector))
                                                         (doseq [retry-msg (persist/retry-cache-seq connector)]
                                                           (when retry-msg
                                                             (warn "Retry messages for " (:topic retry-msg) " " (:key-val retry-msg))
                                                             (if (coll? (:v retry-msg))
                                                               (doseq [bts (retry-msg-bts-seq retry-msg)]
                                                                 (send-msg connector2 (:topic retry-msg) bts))
                                                               (warn "Invalid retry value " retry-msg))

                                                             (persist/delete-from-retry-cache connector (:key-val retry-msg)))))
                                                       (catch NullPointerException npe
                                                         (when-not (persist/closed? connector)
                                                           (error npe npe)))
                                                       (catch Exception e
                                                         (when-not (.get shutdown-flag)
                                                           (error e e)))))

        ]

    ;;important close needs to call close! on msg-buff
    (async-handler async-ctx
                   (->ProducerState conf
                                    activity-counter
                                    send-cache
                                    retry-cache
                                    metadata-connector
                                    ;send-connector
                                    producer-error-ch)
                   msg-buff
                   async-latch)


    (.scheduleWithFixedDelay scheduled-service ^Runnable (fn [] (try (update-metadata) (catch Exception e (do (error e e)
                                                                                                              (async/>!! metadata-error-ch e))))) 0 10000 TimeUnit/MILLISECONDS)

    (->
      connector2
      (assoc :persist-delay-thread persist-delay-thread)
      (assoc :async-latch async-latch))))

(defrecord KafkaClientService [brokers conf client]
  component/Lifecycle

  (start [component]
    (if (:client component)
      component
      (let [c (create-connector brokers conf)]
        (assoc component :client c))))

  (stop [component]
    (if (:client component)
      (try
        (close (:client component))
        (catch Exception e (error e e))
        (finally (dissoc component :client)))
      component)))

(defn create-client-service [brokers conf]
  (->KafkaClientService brokers conf nil))