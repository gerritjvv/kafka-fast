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
  (:import [java.util.concurrent.atomic AtomicInteger AtomicLong AtomicBoolean]
           [kafka_clj.response ProduceResponse]
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit ThreadFactory)
           (java.io IOException)))

(defonce ^Long PRODUCER_ERROR_BACKOFF_DEFAULT 2000)

(defrecord ErrorCtx [code topic msgs])
(defrecord ProducerState [conf
                          ^AtomicLong activity-counter
                          retry-cahe producers-cache-ref
                          metadata-producers-ref
                          blacklisted-producers-ref
                          brokers-metadata-ref
                          producer-error-ch])

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
    (not (-> @(:blacklisted-producers-ref state) (get host) (get port)))
    (not= error-code 5)
    (not= error-code -1)))

(defmacro kafka-response [connector buff-ch producer-error-ch producer conf v]
           `(try
              (if (instance? ProduceResponse ~v)
                (let [{:keys [~'correlation-id ~'topic ~'partition ~'offset]} ~v]
                  ;(debug "produce response " v)
                  (if (> (:error-code ~v) 0)
                    (if (:send-cache ~connector)
                      (handle-send-message-error
                        clojure.core.async/>! ~buff-ch ~producer-error-ch
                        (RuntimeException. (str "Response error " (:error-code ~v)))
                        ~producer ~conf ~'topic ~'partition ~'offset (persist/get-sent-message ~connector ~'topic ~'partition ~'correlation-id))
                      (error "Message received (even though acks " (:acks ~conf) " msg " ~v))) ;
                  (persist/remove-sent-message ~connector ~'topic ~'partition ~'correlation-id)))
              (catch Exception ~'e (error ~'e ~'e))))

(defn send-msg
  "Sends a message async and returns false if the connection is closed
  Any errors uncrecoverable errors shuold be readh from the producer-error-ch"
  [{:keys [state] :as connector} topic ^bytes bts]
  (if (and (-> state :conf :batch-fail-message-over-limit) (>= (count bts) ^long (-> state :conf :batch-byte-limit)))
    (throw (RuntimeException. (str "The message size [ " (count bts)  " ] is larger than the configured batch-byte-limit [ " (get-in state [:conf :batch-byte-limit]) "]")))
    (if (first (filter (partial healthy-partition-rc? state) (get @(:brokers-metadata-ref state) topic)))
      (if-not (async/>!! (:msg-ch connector) (produce/message topic -1 bts)) (throw (RuntimeException. "The client has been shutdown")))
      (throw (RuntimeException. (str "No healthy brokers available for topic " topic " " connector))))))

(defn close [{{:keys [^AtomicLong activity-counter producers-cache-ref metadata-pr scheduled-service retry-cache]} :state
              bootstrap-brokers :bootstrap-brokers
              msg-ch :msg-ch}]
  "Close all producers and channels created for the connected"
  ;only close when all activity has ceased
  ;close
  (async/close! msg-ch)
  (loop [v (.get activity-counter)]
    (Thread/sleep 500)
    (if (not= v (.get activity-counter))
      (recur (.get activity-counter))))

  (doseq [producer @producers-cache-ref]
    (try
      (produce/shutdown producer)
      (catch Exception e (error e e))))

  (.shutdownNow ^ScheduledExecutorService scheduled-service)

  (doseq [producer @bootstrap-brokers]
    (try
      (produce/shutdown producer)
      (catch Exception e (error e e))))

  (persist/close-retry-cache retry-cache))


(defn producer-error-ch [connector]
  (get-in connector [:state :producer-error-ch]))

(defn- retry-msg-bts-seq [retry-msg]
  (let [msg (:v retry-msg)]
    (if (or (record? msg) (map? msg))
      [(:bts msg)]
      (map :bts msg))))

(defn- get-metadata-error-ch [connector]
  (:metadata-error-ch connector))

(defn- blacklist!
  "Add the host+port to the blacklisted-producers-ref and returns the ref"
  [blacklisted-producers-ref {:keys [host port] :as partition-rc}]
  (dosync (commute blacklisted-producers-ref assoc-in [host port] partition-rc))
  blacklisted-producers-ref)

(defn- ^ProducerState  unrecoverable-error
  "At this point we can only notify the producer-error-ch and save the messages to disk"
  [^ProducerState producer-state {:keys [code topic msgs] :as error-ctx}]
  (prn "unrecoverable error " code  " for topic " topic " messages " (count msgs))
  (try
    (persist/write-to-retry-cache producer-state topic msgs)
    (catch Exception e (error e (str "Fatal error, cannot persist messages for topic " topic " to retry cache messages " (count msgs)))))
  (async/>!! (:producer-error-ch producer-state) error-ctx)

  producer-state)

(defn- ^ProducerState error-no-partition
  [state topic msgs]
  (unrecoverable-error state (->ErrorCtx :no-partitions topic msgs)))

(defn- ^ProducerState error-no-producer
  [state topic msgs]
  (unrecoverable-error state (->ErrorCtx :no-producer topic msgs)))

(defn- rand-nth' [v] (when-not (empty? v) (rand-nth v)))

(defn select-partition-rc
  "Returns a partition-rc {:host :port :error-code} otherwise nil"
  [^ProducerState state topic]
  (if-let [partition-rc (rand-nth' (get @(:brokers-metadata-ref state) topic))]
    (if (healthy-partition-rc? state partition-rc)
      partition-rc
      (rand-nth' (filter (partial healthy-partition-rc? state) (get @(:brokers-metadata-ref state) topic))))))

(defn- null-safe-deref [d] (when d @d))

(defn- get-or-create-producer!
       "If a producer is not present use dosync and commute to create a delayed producer
        returns [producers-cache-ref producer]  note producer can be nil"
       [^ProducerState state {:keys [host port]}]
       (let [producers-cache-ref (:producers-cache-ref state)]
         (try
           (if-let [prod (null-safe-deref (-> producers-cache-ref deref (get host) (get port)))]
             [producers-cache-ref prod]
             (let [cache (dosync
                           (commute
                             producers-cache-ref
                             (fn [prod-cache]
                               (if-not (get-in prod-cache [host port])
                                 (assoc-in prod-cache [host port] (delay
                                                                    (do
                                                                      (prn "Creating producer " host " " port)
                                                                      (produce/producer host port))))
                                 prod-cache))))]

               [producers-cache-ref (null-safe-deref (-> producers-cache-ref deref (get host) (get port)))]))
           (catch Exception e
             (do
               (blacklist! (:blacklisted-producers-ref state) {:host host :port port})
               (.printStackTrace e)
               (error e e)
               [producers-cache-ref nil])))))

(declare handle-async-topic-messages)

(defn- send-data [^ProducerState state partition-rc topic msgs]
  (let [[producers-cache-ref prod] (get-or-create-producer! state partition-rc)]
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
                (produce/shutdown prod)
                (dosync (alter producers-cache-ref dissoc-in [(:host partition-rc) (:port partition-rc)]))
                (catch Exception e (error e e))))

            (Thread/sleep PRODUCER_ERROR_BACKOFF_DEFAULT)
            (handle-async-topic-messages (assoc state :blacklisted-producers-ref (blacklist! (:blacklisted-producers-ref state) partition-rc)) topic msgs))))
      (handle-async-topic-messages (assoc state :blacklisted-producers-ref (blacklist! (:blacklisted-producers-ref state) partition-rc)) topic msgs))))

(defn- ^ProducerState handle-async-topic-messages
  "Send messages from the same topic to a producer"
  [^ProducerState state topic msgs]
  (if-let [partition-rc (select-partition-rc state topic)]
    (send-data state partition-rc topic (mapv #(assoc % :partition (:id partition-rc)) msgs))
    (error-no-partition state topic msgs)))

(defn- handle-async-messages [^ProducerState producer-state msgs]
  (reduce (fn [state [topic topic-msgs]]
            (handle-async-topic-messages state topic topic-msgs)) producer-state (group-by :topic msgs)))


(defn- async-handler [^ProducerState producer-state msg-ch msg-buff]
  (async/thread
    (do
      (loop [state producer-state]
        (when-let [msgs (async/<!! msg-buff)]
          (recur (if (empty? msgs) state (handle-async-messages state msgs))))))))

(defn create-connector
  "Creates a connector for sending to kafka
   All sends are asynchronous, unrecoverable errors are saved to a local retry cache and
   also notified to the producer-error-ch with data of type ErrorCtx"
                       [bootstrap-brokers {:keys [acks
                                                  batch-num-messages
                                                  queue-buffering-max-ms
                                                  batch-fail-message-over-limit batch-byte-limit blacklisted-expire producer-retry-strategy topic-auto-create flush-on-write]
                                           :or {batch-num-messages 25
                                                queue-buffering-max-ms 500
                                                blacklisted-expire 10000 acks 0 batch-fail-message-over-limit true batch-byte-limit 10485760
                                                topic-auto-create true flush-on-write false}
                                           :as conf}]
  (let [

        ^ScheduledExecutorService scheduled-service (Executors/newSingleThreadScheduledExecutor (daemon-thread-factory))

        metadata-producers-ref (ref (filter (complement nil?) (map #(delay (produce/metadata-request-producer (:host %) (:port %) conf)) bootstrap-brokers)))
        brokers-metadata-ref (ref (meta/get-metadata @metadata-producers-ref conf))

        ;blacklist producers and no use them in metdata updates or sending, they expire after a few seconds
        blacklisted-producers-ref (ref (cache/create-cache :expire-after-write blacklisted-expire))
        blacklisted-metadata-producers-ref (ref (cache/create-cache :expire-after-write blacklisted-expire))

        producers-cache-ref (ref {})

        metadata-error-ch (async/chan (async/dropping-buffer 10))
        producer-error-ch (async/chan (async/dropping-buffer 10))

        send-cache (if (> acks 0) (persist/create-send-cache conf))
        retry-cache (persist/create-retry-cache conf)

        activity-counter (AtomicLong. 0)
        state {
               :activity-counter activity-counter
               :retry-cache retry-cache
               :producers-cache-ref producers-cache-ref
               :brokers-metadata-ref brokers-metadata-ref
               :producer-error-ch producer-error-ch
               :blacklisted-producers-ref blacklisted-producers-ref
               :scheduled-service scheduled-service
               :conf (assoc conf :batch-fail-message-over-limit batch-fail-message-over-limit :batch-byte-limit batch-byte-limit :topic-auto-create topic-auto-create)}


        update-metadata (fn [& {:keys [timeout-ms producers]}]
                          (if-let [metadata (meta/get-metadata (or producers @metadata-producers-ref) (if timeout-ms (assoc conf :metadata-timeout timeout-ms) conf) :blacklisted-metadata-producers-ref blacklisted-metadata-producers-ref)]
                            (ref-set brokers-metadata-ref metadata)
                            (error (str "No metadata found"))))

        connector {:bootstrap-brokers metadata-producers-ref :send-cache send-cache :retry-cache retry-cache
                   :producers-cache-ref producers-cache-ref
                   :flush-on-write flush-on-write
                   :state state}


                  ;;every 5 seconds check for any data in the retry cache and resend the messages
				retry-cache-ch (futils/fixdelay-thread 1000
										      (try
                            (do
                             (doseq [retry-msg (persist/retry-cache-seq connector)]
                                 (do (warn "Retry messages for " (:topic retry-msg) " " (:key-val retry-msg))
                                   (if (coll? (:v retry-msg))
                                     (doseq [bts (retry-msg-bts-seq retry-msg)]
                                       (send-msg connector (:topic retry-msg) bts))
                                     (warn "Invalid retry value " retry-msg))

										                  (persist/delete-from-retry-cache connector (:key-val retry-msg)))))
										         (catch Exception e (error e e))))

        msg-ch (async/chan 100)
        msg-buff (futils/buffered-chan msg-ch batch-num-messages queue-buffering-max-ms 5 (produce/flush-on-byte->fn batch-byte-limit))

        connector2 (assoc connector :msg-ch msg-ch :retry-cache-ch retry-cache-ch :metadata-error-ch metadata-error-ch :update-metadata update-metadata)

        ]

    (async-handler (->ProducerState conf activity-counter retry-cache producers-cache-ref metadata-producers-ref blacklisted-producers-ref brokers-metadata-ref producer-error-ch) msg-ch msg-buff)
    (.scheduleWithFixedDelay scheduled-service ^Runnable (fn [] (try (update-metadata) (catch Exception e (do (error e e)
                                                                                                              (async/>!! metadata-error-ch e))))) 0 10000 TimeUnit/MILLISECONDS)

    connector2))
 
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