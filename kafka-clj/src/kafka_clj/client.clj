(ns kafka-clj.client
  (:require [fun-utils.core :refer [star-channel buffered-chan fixdelay-thread apply-get-create stop-fixdelay thread-seq go-seq]]
            [kafka-clj.produce :refer [producer metadata-request-producer send-messages message shutdown]]
            [kafka-clj.metadata :refer [get-metadata]]
            [kafka-clj.msg-persist :refer [get-sent-message close-send-cache create-send-cache close-send-cache remove-sent-message
                                           create-retry-cache write-to-retry-cache retry-cache-seq close-retry-cache delete-from-retry-cache]]
            [clojure.tools.logging :refer [error info debug warn]]
            [com.stuartsierra.component :as component]
            [clj-tuple :refer [tuple]]
            [clojure.core.cache :as cache]
            [clojure.core.async :refer [chan >! >!! <! <!! thread go close! dropping-buffer] :as async])
  (:import [java.util.concurrent.atomic AtomicInteger AtomicLong]
           [kafka_clj.response ProduceResponse]
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit)))

(declare close-producer-buffer!)

(defn- get-partition-count [topic brokers-metadata]
  (count (get brokers-metadata topic)))

(defn smart-deref [x]
  (if
    (instance? clojure.lang.IDeref x) (deref x)
    x))

(defn smart-get [x k]
  (if (or (delay? x) (future? x))
    (get @x k)
    (get x k)))


(defn flush-on-byte->fn
  "Returns a check-f for buffered-chan that will flush if the accumulated byte count is bigger than that of byte-limit"
  [^long byte-limit]
  (fn
    ([] 0)
    ([^long byte-cnt msg]
      (let [total-cnt (+ byte-cnt (count (:bts msg)))]
        (if (>= total-cnt byte-limit)
          (tuple true 0)
          (tuple false total-cnt))))))

(defn do-metadata-update [{:keys [update-metadata]}]
  (update-metadata :timeout-ms 1000))

(defn select-rr-partition! [topic {:keys [topic-partition-ref brokers-metadata] :as state}]
  "If a counter does not exist in the topic-partition-ref it will be created
   and set using commute, the return result is an increment on the topic counter
   This operation is fast and is benchmarked at over 2 million ops per second
   usage: 
    (def state {:topic-partition-ref (ref {})})
    (select-rr-partition! b {:topic-partition-ref state})
   "
  (let [^Long partition-count (get-partition-count topic @brokers-metadata)]
   (if (> partition-count 0)	    
		  (if-let [^AtomicLong pcounter (get @topic-partition-ref topic)]
		    (mod ^Long (.getAndIncrement pcounter) partition-count)
		    (do 
		     (dosync 
		           (commute topic-partition-ref (fn [x] 
		                                          (assoc x topic (AtomicLong. 0)))))
		     (select-rr-partition! topic state)))
    (if (-> state :conf :topic-auto-create)
      0
      (do
        (error "No topic found " topic " in state map " topic-partition-ref)
        (error "broker-meta " brokers-metadata)
        (throw (RuntimeException.
                 (str "The topic " topic " does not exist, please create it first. See http://kafka.apache.org/documentation.html"))))))))

(defn get-broker-from-metadata [topic partition brokers-metadata]
  (-> brokers-metadata (get topic) (get partition)))

(defn random-select-broker [topic brokers-metadata]
  "Select a broker over all the registered topics randomly"
  (if-let [brokers (get brokers-metadata topic)]
    (rand-nth brokers)
    (->> brokers-metadata keys (rand-nth) (get brokers-metadata) rand-nth)))

  
(defn select-broker
  "Try to find the broker in the brokers-metadata by topic and partition,
    if no broker can be found, a random select is done, if still
    no broker is found nil is returned"
  [topic partition brokers-metadata & {:keys [topic-auto-create]}]
  (if-let [broker (get-broker-from-metadata topic partition @brokers-metadata)]
    [partition broker]
    [0 (random-select-broker topic @brokers-metadata)]))

(defn send-to-buffer [{:keys [ch-source]} msg]
  (>!! ch-source msg))

(defn get-latest-msg [messages]
  "Get the latest message taking care if messages is a seq or vector"
  (cond 
    (seq? messages) (first messages)
    (vector? messages) (last messages)
    :else (throw (RuntimeException. (str "Collection type not expected here " messages)))))

(defn count-bytes 
  "An accumulator state function that counts the number of bytes of messages in a vector
   and if higher than v returns [true acc] [false acc]"
  ([] 0)
  ([^Long acc messages ^Long v]
    (let [acc2 (+ ^Long acc (count ^"[B" (:bts (get-latest-msg messages))))]
      (tuple (>= acc2 v) acc2))))
    
(defonce t (tuple false false))
(defn- always-false ([] false) ([_ _] t))

(defmacro handle-send-message-error [async buff-ch producer-error-ch e producer conf topic partition offset v]
  `(do
     (error ~e ~e)
     (prn "handle-send-message-error: v " ~v)
     (~async ~producer-error-ch {:key-val (str ~topic ":" ~partition) :error ~e :producer {:producer ~producer :buff-ch ~buff-ch}
                             :offset ~offset :v ~v :topic ~topic})))


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
                        ~producer ~conf ~'topic ~'partition ~'offset (get-sent-message ~connector ~'topic ~'partition ~'correlation-id))
                      (error "Message received (even though acks " (:acks ~conf) " msg " ~v))) ; TODO send error back to connection and exit
                  (remove-sent-message ~connector ~'topic ~'partition ~'correlation-id)))
              (catch Exception ~'e (error ~'e ~'e))))

(defmacro handle-error [buff-ch producer-error-ch producer conf topic partition v]
  `(let [[e# v#] ~v]
     (warn "ERROR: ERROR_CH: e " e# " v " v#)
     (if (fn? v#)
       (handle-send-message-error
         clojure.core.async/>! ~buff-ch ~producer-error-ch
         (RuntimeException. (str "Client tcp error " e#))
         ~producer ~conf ~topic ~partition -1 v#)
       (error "Cannot react on message " v#))

     (error e# e#)))

(defn- cached-producer-from-connector
  "Called by create-producer-buffer and caches the producer wrapped in a delay in the producers-cache
   The create connector uses a closure to remove closed producers on producer error"
  [{:keys [producers-cache] :as connector} conf host port]
  {:pre [producers-cache]}
  (deref
    (if-let [p
             (get @producers-cache (str host ":" port))]
      p
      (get
        (dosync (alter producers-cache assoc (str host ":" port) (delay (producer host port conf))))
        (str host ":" port)))))

(defn create-producer-buffer [connector topic partition producer-error-ch {:keys [host port]} {:keys [batch-num-messages queue-buffering-max-ms batch-byte-limit batch-fail-message-over-limit]
                                                                                               :or
                                                                                               {batch-num-messages 1000 queue-buffering-max-ms 500 batch-byte-limit 10485760} :as conf}]
  "Creates a producer and buffered-chan with a go loop that will read off the buffered chan and send to the producer.
   A map with keys :producer ch-source and buff-ch is returned"
  (info "CREATING PRODUCER_BUFFER " topic " " partition " : " host ": " port)

  (let [producer (cached-producer-from-connector connector conf host port) ;(producer host port conf)
        c (:client producer)
        ch-source (chan 100)
        read-ch (-> producer :client :read-ch)
        buff-ch (buffered-chan ch-source batch-num-messages queue-buffering-max-ms 2 (flush-on-byte->fn batch-byte-limit))

        error-ch (:error-ch c)]
    (info "CREATED PRODUCER_BUFFER " topic " " partition " : " host ": " port)
   
    ;if a response from the server (only when ack > 0)
    ; if error-codec > 0 then handle the error
    ; else remove the message from the cache
    ;connector buff-ch producer-error-ch producer conf v]
    (thread-seq
      (fn [v]
        (kafka-response connector buff-ch producer-error-ch producer conf v))
      read-ch)

    (thread-seq
      (fn [v]
        (handle-error buff-ch producer-error-ch producer conf topic partition v))
      error-ch)

    ;send buffered messages
    ;if any exception handle the error
    (thread-seq
      (fn [v]
        (when (> (count v) 0)
          (try
            (send-messages connector producer conf v)
            (catch Exception e (handle-send-message-error >!! buff-ch producer-error-ch e producer conf topic partition -1 v)))))
      buff-ch)


    {:host host
     :port port
     :producer producer
     :ch-source ch-source
     :buff-ch buff-ch}))
  
(defn add-remove-on-error-listener! [producers-ref topic partition producer-buffer brokers-metadata]
  "If any error is read on the client error-ch this producer this removed from the producers-ref"
  ;this loop will block and on the first error, close and remove the producer, then exit the loop
  (let [error-ch (-> producer-buffer :producer :client :error-ch)]
	   (go
      (if-let [error-val (<! error-ch)]
       (do
         ;(error (first error-val) (first error-val))
         (prn "removing producer for " topic ":" partition)
         (info "removing producer for " topic ":" partition)
         (thread
           (try
             (shutdown (:producer producer-buffer))
             (catch Exception e (error e e))))
         ;remove from ref
         (dosync 
           (alter producers-ref (fn [m]
                                  (prn "removing producers ref: " topic partition)
                                  (dissoc (get m topic) partition)
                                  ))))))))
     
  
(defn find-hashed-connection
  "Finds the value with a key that when hashed and moded against upper-limit is the same for k
   this is an optimization for hashed partitioning where the original keys are stored in the producers-ref
   i.e. topic:partition but the connections are created on (mod (hash k) upper-limit)"
  [broker k producers upper-limit]
  (let [hashed-k (hash k)
	     [_ producer] (first 
                       (filter (fn [[producer-k {:keys [host port]}]]
                                 (and (= host (smart-get broker :host)) (= port (smart-get broker :port))
                                    (= (mod (hash producer-k) upper-limit) (mod hashed-k upper-limit)))) producers))]
      producer))

	             
(defn find-producer [broker k producers]
  (let [[_ producer] (first 
                       (filter (fn [_ v] (= (smart-get v :host) (smart-get broker :host)))
                         producers))]
        producer))

;perf improvment for blacklisted exception lookup
(defonce string-cache (atom {}))

(defn- string-cache-lookup [host port]
  (if-let [s (-> string-cache deref (get host) (get port))]
    s
    (do
      (swap! string-cache (fn [x] (assoc-in x [host port] (str host ":" port))))
      (str host ":" port))))

(defn- exception-if-blacklisted
  "blacklisted-producers keys are (str host ':' port)
   Throws a runtime exception if  host port combindation is found"
  [{:keys [host port] :as producer} blacklisted-producers]
  (if (get blacklisted-producers (string-cache-lookup host port))
    (throw (RuntimeException. (str "Black listed producer: " host ":" port)))
    producer))

(defn- add-producer! [connector topic partition {:keys [brokers-metadata producers-ref producer-error-ch conf] :as state}]
  (dosync (alter producers-ref
                 (fn [x]
                   (info "adding producer to ref:  topic " topic " partition " partition)
                   (if-let [producer (-> producers-ref deref (get topic) (get partition))] ;use get get for speed
                     x ; return map the producer already exists
                     (let [[partition broker] (select-broker topic partition brokers-metadata :topic-auto-create (:topic-auto-create conf))]
                       (if-let [producer (find-hashed-connection broker (str topic ":" partition) @producers-ref (get conf :producer-connections-max 2))]
                         (assoc-in x [topic partition] producer) ; found a producer based on hash and broker for the same partition
                         (assoc-in x [topic partition] (delay ;else create a new producer
                                                         (let [producer-buffer (create-producer-buffer connector topic partition producer-error-ch broker conf)]
                                                           (add-remove-on-error-listener! producers-ref topic partition producer-buffer brokers-metadata)
                                                           producer-buffer))))))))))

(defn select-producer-buffer! [connector topic partition {:keys [producers-ref brokers-metadata producer-error-ch conf] :as state}]
  "Select a producer or create one,
   if no broker can be found a RuntimeException is thrown"
  (if-let [producer (-> producers-ref deref (get topic) (get partition))]
    @producer
    (do
      (add-producer! connector topic partition state)
      (recur connector topic partition state))))


(defn- send-msg-retry [{:keys [state] :as connector} {:keys [topic] :as msg}]
  "Try sending the message to any of the producers till all of the producers have been trieds"
   (do-metadata-update connector)
   (let [partitions  (let [partitions(-> state :brokers-metadata deref (get topic)) ]
                       (if (or (empty? partitions) (= partitions 0))
                         (if (-> state :conf :topic-auto-create)
                           [0]
                           (throw (RuntimeException. (str "No topics available for topic " topic " partitions " partitions))))
                         partitions))]

     (prn "using partitions " partitions)
      (loop [partitions-1 partitions i 0]
        (if-let [partition (first partitions-1)]
          (let [sent (try
						            (let [producer-buffer (exception-if-blacklisted (select-producer-buffer! connector topic i state) @(:blacklisted-producers-ref state))]
                          (send-to-buffer producer-buffer msg)
						              true)
						            (catch Exception e (do (.printStackTrace e) (error e e) false)))]
            (if (not sent)
              (recur (rest partitions-1) (inc i))))
          (throw (RuntimeException. (str "The message for topic " topic " could not be sent")))))))

(defn send-msg [{:keys [state] :as connector} topic ^bytes bts]
  (if (and (get-in state [:conf :batch-fail-message-over-limit]) (>= (count bts) ^long (get-in state [:conf :batch-byte-limit])))
    (throw (RuntimeException. (str "The message size [ " (count bts)  " ] is larger than the configured batch-byte-limit [ " (get-in state [:conf :batch-byte-limit]) "]")))
    (if (> (-> state :brokers-metadata deref count) 0)
      (let [partition (select-rr-partition! topic state)
            msg (message topic partition bts)]
        (try
          (send-to-buffer (exception-if-blacklisted (select-producer-buffer! connector topic partition state) @(:blacklisted-producers-ref state)) msg)
          (catch Exception e
            (do
                (warn "error while sending message")
                (error e e)
                ;here we try all of the producers, if all fail we throw an exception
                (send-msg-retry connector msg)))))
      (throw (RuntimeException. (str "No brokers available: " connector))))))

(defn close-producer-buffer! [{:keys [producer ch-source]}]
  (try
		    (do
          (if ch-source
            (close! ch-source))
          (if producer
            (shutdown producer)))
      (catch Exception e (error e (str "Error while shutdown " producer)))))

(defn close [{:keys [state] :as connector}]
  "Close all producers and channels created for the connected"
  (.shutdown ^ScheduledExecutorService (:scheduled-service state))
  
  (close-retry-cache connector)
  (close-send-cache connector)
 
  (doseq [producer-buffer (deref (:producers-ref state))]
    (close-producer-buffer! (smart-deref producer-buffer))))


(defn producer-error-ch [connector]
  (get-in connector [:state :producer-error-ch]))

(defn- exclude-host [host1 port1]
  (fn [{:keys [host port] :as msg}]
    (not (and (= host host1) (= port port1)))))

(defn retry-msg-bts-seq [retry-msg]
  (let [msg (:v retry-msg)]
    (if (or (record? msg) (map? msg))
      [(:bts msg)]
      (map :bts msg))))

(defn get-metadata-error-ch [connector]
  (:metadata-error-ch connector))


(defn create-connector [bootstrap-brokers {:keys [acks batch-fail-message-over-limit batch-byte-limit blacklisted-expire producer-retry-strategy topic-auto-create] :or {blacklisted-expire 10000 acks 0 batch-fail-message-over-limit true batch-byte-limit 10485760 producer-retry-strategy :default topic-auto-create true} :as conf}]
  (let [
        ^ScheduledExecutorService scheduled-service (Executors/newSingleThreadScheduledExecutor)
        metadata-producers-ref (ref (filter (complement nil?) (map #(delay (metadata-request-producer (:host %) (:port %) conf)) bootstrap-brokers)))
        brokers-metadata (ref (get-metadata @metadata-producers-ref conf))
        _ (if (empty? @brokers-metadata)
            (throw (RuntimeException. (str "No broker metadata could be found for " bootstrap-brokers))))

        ;blacklist producers and no use them in metdata updates or sending, they expire after a few seconds
        blacklisted-producers-ref (ref (cache/ttl-cache-factory {} :ttl blacklisted-expire))
        blacklisted-metadata-producers-ref (ref (cache/ttl-cache-factory {} :ttl blacklisted-expire))

        producers-cache (ref {})                            ;cache (produce host port)instances
        producer-error-ch (chan 100)
        metadata-error-ch (chan (dropping-buffer 1))

        producer-ref (ref {})

        send-cache (if (> acks 0) (create-send-cache conf))
        retry-cache (create-retry-cache conf)
        state {:producers-ref producer-ref

               :brokers-metadata brokers-metadata
               :topic-partition-ref (ref {})
               :producer-error-ch producer-error-ch
               :blacklisted-producers-ref blacklisted-producers-ref
               :conf (assoc conf :batch-fail-message-over-limit batch-fail-message-over-limit :batch-byte-limit batch-byte-limit :topic-auto-create topic-auto-create)}
        ;go through each producer, if it does not exist in the metadata, the producer is closed, otherwise we keep it.
        update-producers (fn [metadata m] 
                               (into {} (filter (complement nil?)
                                            (map (fn [[k producer-buffer]]
								                                                    (let [[topic partition] (clojure.string/split k #"\:")
								                                                          host (smart-get producer-buffer :host)]
								                                                      (if (some #(= (:host %) host) (smart-get metadata topic))
								                                                        [k producer-buffer]
								                                                        (do
                                                                          (warn "closing producer " host " topic " topic " prodbuffer " producer-buffer)
								                                                          (close-producer-buffer! (smart-deref producer-buffer))
								                                                          nil))))
                                                 m))))

        ;from the producers-ref if any new brokers add them to the metadata-producers-ref
        ;this will only include

        ;try each metadata-producer-ref entry in search of metadata
        ;update the producers (ref) with the new metadata found
        update-metadata (fn [& {:keys [timeout-ms producers]}]
                          (if-let [metadata (get-metadata (or producers @metadata-producers-ref) (if timeout-ms (assoc conf :metadata-timeout timeout-ms) conf) :blacklisted-metadata-producers-ref blacklisted-metadata-producers-ref)]
                            (dosync
                              (alter brokers-metadata (fn [x] metadata))
                              ;causes producers to be removed and re-created causing memory leaks
                              ;(alter producer-ref (fn [x] (update-producers metadata x)))
                              )
                            (error (str "No metadata found"))))

        connector {:bootstrap-brokers metadata-producers-ref :send-cache send-cache :retry-cache retry-cache
                   :producers-cache producers-cache
                   :state (assoc state :scheduled-service scheduled-service) }
        
                  ;;every 5 seconds check for any data in the retry cache and resend the messages
				retry-cache-ch (fixdelay-thread 1000
										      (try
                            (do
                             (doseq [retry-msg (retry-cache-seq connector)]
                                 (do (warn "Retry messages for " (:topic retry-msg) " " (:key-val retry-msg))
                                   (if (coll? (:v retry-msg))
                                     (doseq [bts (retry-msg-bts-seq retry-msg)]
                                       (send-msg connector (:topic retry-msg) bts))
                                     (warn "Invalid retry value " retry-msg))
												              
										                  (delete-from-retry-cache connector (:key-val retry-msg)))))
										         (catch Exception e (error e e))))]

    (.scheduleWithFixedDelay scheduled-service ^Runnable (fn [] (try (update-metadata) (catch Exception e (do (error e e)
                                                                                                              (>!! metadata-error-ch e))))) 0 10000 TimeUnit/MILLISECONDS)
    ;listen to any producer errors, this can be sent from any producer
    ;update metadata, close the producer and write the messages in its buff cache to the 
    (if (= producer-retry-strategy :default)
      (thread-seq
        (fn [error-val]
          (try
            (warn "producer error producer-error-ch")
            (let [{:keys [key-val producer v topic]} error-val
                  host (-> producer :producer :host)
                  port (-> producer :producer :port)]
              ;persist to retry cache
              (.printStackTrace ^Throwable (:error error-val))

              (dosync
                (alter blacklisted-producers-ref (fn [m] (assoc m (str host ":" port) true))))

              (dosync
                (alter producers-cache dissoc (str host ":" port))
                (alter producer-ref (fn [m] (dissoc (get m host) port))))

              ;remove
              (if (coll? v)                                   ;only write valid messages to the retry cache
                (write-to-retry-cache connector topic v)
                (warn "Could not send message to retry cache: invalid message " v))

              ;close producer and cause the buffer to be flushed
              (close-producer-buffer! (smart-deref (:producer producer)))
              (try
                (update-metadata :producers (filter (exclude-host host port) metadata-producers-ref) :timeout-ms 5000)
                (catch Exception e (do
                                     (error e e)
                                     (>!! metadata-error-ch e)))))
            (catch Exception e (error e e)))) producer-error-ch))

     (assoc connector :retry-cache-ch retry-cache-ch :metadata-error-ch metadata-error-ch :update-metadata update-metadata)))
 
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