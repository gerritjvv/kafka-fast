(ns kafka-clj.client
  (:require
            [fun-utils.core :as futils]
            [fun-utils.threads :as fthreads]
            [fun-utils.cache :as cache]
            [kafka-clj.produce :as produce]
            [kafka-clj.response :as kafka-resp]
            [kafka-clj.tcp :as tcp]
            [kafka-clj.metadata :as meta]
            [kafka-clj.msg-persist :as persist]
            [clojure.tools.logging :refer [error info debug warn]]
            [com.stuartsierra.component :as component]
            [clj-tuple :refer [tuple]]
            [clojure.core.async :as async])
  (:import [java.util.concurrent.atomic AtomicLong]
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit ThreadFactory ExecutorService CountDownLatch)
           (java.io IOException ByteArrayInputStream DataInputStream)))

(defonce ^Long PRODUCER_ERROR_BACKOFF_DEFAULT 2000)

(defrecord ErrorCtx [code topic msgs])
(defrecord ProducerState [conf
                          ^AtomicLong activity-counter
                          send-cache
                          retry-cahe
                          producers-cache-ref
                          metadata-producers-ref
                          blacklisted-producers-ref
                          brokers-metadata-ref
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

(defn- healthy-partition-rc? [^ProducerState state {:keys [host port error-code]}]
  (and
    host
    port
    (not (-> @(:blacklisted-producers-ref state) (get host) (get port)))
    (not= error-code 5)
    (not= error-code -1)))

(defn send-msg
  "Sends a message async and returns false if the connection is closed
  Any errors uncrecoverable errors shuold be readh from the producer-error-ch"
  [{:keys [state] :as connector} topic ^bytes bts]
  (if (and (-> state :conf :batch-fail-message-over-limit) (>= (count bts) ^long (-> state :conf :batch-byte-limit)))
    (throw (RuntimeException. (str "The message size [ " (count bts)  " ] is larger than the configured batch-byte-limit [ " (get-in state [:conf :batch-byte-limit]) "]")))
    (if (first (filter (partial healthy-partition-rc? state) (get @(:brokers-metadata-ref state) topic)))
      (if-not (async/>!! (:msg-ch connector) (produce/message topic -1 bts)) (throw (RuntimeException. "The client has been shutdown")))
      (throw (RuntimeException. (str "No healthy brokers available for topic " topic " " connector))))))

(defn close [{{:keys [^AtomicLong activity-counter metadata-producers-ref producers-cache-ref scheduled-service retry-cache]} :state
              msg-ch :msg-ch
              persist-delay-thread :persist-delay-thread
              async-latch :async-latch}]
  "Close all producers and channels created for the connected"
  ;only close when all activity has ceased
  ;close
  (async/close! msg-ch)
  (loop [v (.get activity-counter)]
    (Thread/sleep 500)
    (if (not= v (.get activity-counter))
      (recur (.get activity-counter))))

  (doseq [producers (->> @producers-cache-ref vals (map vals) flatten)]
    (doseq [producer @producers]
      (try
        (produce/shutdown producer)
        (catch Exception e (error e e)))))

  (.shutdownNow ^ScheduledExecutorService scheduled-service)

  (doseq [[_ producer] @metadata-producers-ref]
    (try
      (produce/shutdown @producer)
      (catch Exception e (error e e))))

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

(defn- blacklist!
  "Add the host+port to the blacklisted-producers-ref and returns the ref"
  [blacklisted-producers-ref {:keys [host port] :as partition-rc}]
  (dosync (commute blacklisted-producers-ref assoc-in [host port] partition-rc))
  blacklisted-producers-ref)

(defn- ^ProducerState  unrecoverable-error
  "At this point we can only notify the producer-error-ch and save the messages to disk"
  [^ProducerState producer-state {:keys [code topic msgs] :as error-ctx}]
  (error "unrecoverable error " code  " for topic " topic " messages " (count msgs) " saving to retry cache and retry at a later time")
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
  "Returns a partition-rc {:host :port :error-code} otherwise nil"
  [^ProducerState state topic]
  (if-let [partition-rc (rand-nth' (get @(:brokers-metadata-ref state) topic))]
    (if (healthy-partition-rc? state partition-rc)
      partition-rc
      (rand-nth' (filter (partial healthy-partition-rc? state) (get @(:brokers-metadata-ref state) topic))))))

(defn- null-safe-deref [d] (when d @d))

(defn kafka-response
  "Handles the response input stream for each producer.
   Takes the state and resp:ProduceResponse.
   If the error-code > 0 and a send-cache is in state, the messages are retrieved from the persist and if still in persist,
   the messages are sent using handle-async-topic-messages."
  [^ProducerState state resp]
  (let [error-code (:error-code resp)                       ;;dont use {:keys []} here some weirdness was seend that caused
        topic (:topic resp)                                 ;;java.lang.IllegalArgumentException: No value supplied for key: kafka_clj.response.ProduceResponse
        partition (:partition resp)                         ;;unrolling creates a hashmap and for some reason doesn't get the supplied keys
        correlation-id (:correlation-id resp)]

    (if (and (number? error-code) topic partition)
      (when (> (long error-code) 0)
        (if (:send-cache state)
          (when-let [msg (persist/get-sent-message state topic partition correlation-id)]
            (handle-async-topic-messages state topic (if (coll? msg) msg [msg])))
          (error (str "Message received event though acks < 1 msg " resp))))
      (error (str "Message cannot contain nil values topic " topic " partition " partition " error-code " error-code " resp: " resp " keys " (keys resp))))))

(defn- create-producer
  "Create a producer instance and add a read-async-loop instance that will listen
   on the InputStream and read responses.
   "
  [^ProducerState state host port]
  (let [producer (produce/producer host port)]
    (tcp/read-async-loop! (:client producer)
                          (fn [^"[B" bts]
                            (when (> (count bts) 4)
                              (let [^DataInputStream in (DataInputStream. (ByteArrayInputStream. bts))]
                                (try
                                  (doseq [resp (kafka-resp/in->kafkarespseq in)]
                                    (kafka-response state resp))
                                  (finally
                                    (.close in)))))))
    producer))

(defn- get-or-create-producers!
       "If a producer is not present use dosync and commute to create a delayed producer
        returns [producers-cache-ref producer]  note producer can be nil"
       [^ProducerState state {:keys [host port]}]
       (let [producers-cache-ref (:producers-cache-ref state)]
         (try
           (if-let [prods (null-safe-deref (-> producers-cache-ref deref (get host) (get port)))]
             [producers-cache-ref prods]
             (do
               (dosync
                 (commute
                   producers-cache-ref
                   (fn [prod-cache]
                     (if-not (get-in prod-cache [host port])
                       (assoc-in prod-cache [host port] (delay
                                                          (do
                                                            (info "Creating producer " host " " port)
                                                            [(create-producer state host port) (create-producer state host port)])))
                       prod-cache))))
               [producers-cache-ref (null-safe-deref (-> producers-cache-ref deref (get host) (get port)))]))
           (catch Exception e
             (do
               (blacklist! (:blacklisted-producers-ref state) {:host host :port port})
               (.printStackTrace e)
               (error e e)
               [producers-cache-ref nil])))))

(defn- send-data [^ProducerState state partition-rc topic msgs]
  (let [[producers-cache-ref prods] (get-or-create-producers! state partition-rc)
        prod (rand-nth prods)]
    (if prod
      (try
        (do
          (.incrementAndGet ^AtomicLong (:activity-counter state))
          (produce/send-messages prod (:conf state) msgs)
          (assoc state :producers-cache-ref producers-cache-ref))
        (catch Exception e
          (do
            (when (instance? IOException e)
              (try
                (doseq [prod prods] (produce/shutdown prod))
                (dosync (alter producers-cache-ref dissoc-in [(:host partition-rc) (:port partition-rc)]))
                (catch Exception e (error e e))))

            (Thread/sleep PRODUCER_ERROR_BACKOFF_DEFAULT)
            (handle-async-topic-messages (assoc state :blacklisted-producers-ref (blacklist! (:blacklisted-producers-ref state) partition-rc)) topic msgs))))
      (handle-async-topic-messages (assoc state :blacklisted-producers-ref (blacklist! (:blacklisted-producers-ref state) partition-rc)) topic msgs))))


(defn handle-async-topic-messages
  "Send messages from the same topic to a producer"
  [^ProducerState state topic msgs]
  (if-let [partition-rc (select-partition-rc state topic)]
    (send-data state partition-rc topic (mapv #(assoc % :partition (:id partition-rc)) msgs))
    (error-no-partition state topic msgs)))


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
    (throw (ex-info (str "No metadata could be found from any of the bootstrap brokers provided " bootstrap-brokers) {:type :metadata-exception
                                                                                                                      :bootstrap-brokers bootstrap-brokers}))))

(defn create-connector
  "Creates a connector for sending to kafka
   All sends are asynchronous, unrecoverable errors are saved to a local retry cache and
   also notified to the producer-error-ch with data of type ErrorCtx"
                       [bootstrap-brokers {:keys [acks
                                                  io-threads
                                                  batch-num-messages
                                                  queue-buffering-max-ms
                                                  batch-fail-message-over-limit batch-byte-limit blacklisted-expire topic-auto-create flush-on-write]
                                           :or {batch-num-messages 25
                                                queue-buffering-max-ms 500
                                                io-threads 10
                                                blacklisted-expire 10000 acks 0 batch-fail-message-over-limit true batch-byte-limit 10485760
                                                topic-auto-create true flush-on-write false}
                                           :as conf}]
  (let [

        ^ScheduledExecutorService scheduled-service (Executors/newSingleThreadScheduledExecutor (daemon-thread-factory))
        async-ctx (fthreads/create-exec-service io-threads)                                       ;(fthreads/shared-threads (inc io-threads))
        metadata-producers-ref (ref (meta/bootstrap->producermap bootstrap-brokers conf))

        ;blacklist producers and no use them in metdata updates or sending, they expire after a few seconds
        blacklisted-producers-ref (ref (cache/create-cache :expire-after-write blacklisted-expire))
        blacklisted-metadata-producers-ref (ref (cache/create-cache :expire-after-write blacklisted-expire))

        producers-cache-ref (ref {})

        metadata-error-ch (async/chan (async/dropping-buffer 10))
        producer-error-ch (async/chan (async/dropping-buffer 10))

        send-cache (if (> acks 0) (persist/create-send-cache conf))
        retry-cache (persist/create-retry-cache conf)

        activity-counter (AtomicLong. 0)
        async-latch (CountDownLatch. 1)

        state {
               :activity-counter activity-counter
               :retry-cache retry-cache
               :metadata-producers-ref metadata-producers-ref
               :blacklisted-metadata-producers-ref blacklisted-metadata-producers-ref
               :producers-cache-ref producers-cache-ref
               :producer-error-ch producer-error-ch
               :blacklisted-producers-ref blacklisted-producers-ref
               :scheduled-service scheduled-service
               :conf (assoc conf :batch-fail-message-over-limit batch-fail-message-over-limit :batch-byte-limit batch-byte-limit :topic-auto-create topic-auto-create)}

        brokers-metadata-ref (ref (exception-if-nil! bootstrap-brokers (meta/get-metadata! state conf)))

        update-metadata (fn []
                          (if-let [metadata (meta/get-metadata! state conf)]
                            (dosync (ref-set brokers-metadata-ref metadata))
                            (error (str "No metadata found"))))


        connector {:metadata-producers-ref metadata-producers-ref :send-cache send-cache :retry-cache retry-cache
                   :producers-cache-ref producers-cache-ref
                   :flush-on-write flush-on-write
                   :state (assoc state :brokers-metadata-ref brokers-metadata-ref)}

        msg-ch (async/chan 100)
        msg-buff (futils/buffered-chan msg-ch batch-num-messages queue-buffering-max-ms 5 (produce/flush-on-byte->fn batch-byte-limit))

                  ;;every 5 seconds check for any data in the retry cache and resend the messages
        connector2 (assoc connector :msg-ch msg-ch :metadata-error-ch metadata-error-ch :update-metadata update-metadata)

        persist-delay-thread (futils/fixdelay-thread 1000
                                                     (try
                                                       (do
                                                         (doseq [retry-msg (persist/retry-cache-seq connector)]
                                                           (do (warn "Retry messages for " (:topic retry-msg) " " (:key-val retry-msg))
                                                               (if (coll? (:v retry-msg))
                                                                 (doseq [bts (retry-msg-bts-seq retry-msg)]
                                                                   (send-msg connector2 (:topic retry-msg) bts))
                                                                 (warn "Invalid retry value " retry-msg))

                                                               (persist/delete-from-retry-cache connector (:key-val retry-msg)))))
                                                       (catch Exception e (error e e))))

        ]

    ;;important close needs to call close! on msg-buff
    (async-handler async-ctx
                   (->ProducerState conf
                                    activity-counter
                                    send-cache
                                    retry-cache
                                    producers-cache-ref
                                    metadata-producers-ref
                                    blacklisted-producers-ref
                                    brokers-metadata-ref
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