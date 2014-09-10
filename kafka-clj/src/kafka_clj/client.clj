(ns kafka-clj.client
  (:require [fun-utils.core :refer [star-channel buffered-chan fixdelay apply-get-create stop-fixdelay go-seq]]
            [kafka-clj.produce :refer [producer metadata-request-producer send-messages message shutdown]]
            [kafka-clj.metadata :refer [get-metadata]]
            [kafka-clj.msg-persist :refer [get-sent-message close-send-cache create-send-cache close-send-cache remove-sent-message
                                           create-retry-cache write-to-retry-cache retry-cache-seq close-retry-cache delete-from-retry-cache]]
            [clojure.tools.logging :refer [error info debug]]
            [clj-tuple :refer [tuple]]
            [clojure.core.async :refer [chan >! >!! <! go close!] :as async])
  (:import [java.util.concurrent.atomic AtomicInteger]
           [kafka_clj.response ProduceResponse]))

(declare close-producer-buffer!)

(defn- get-partition-count [topic brokers-metadata]
  (count (get brokers-metadata topic)))


(defn smart-get [x k]
  (if (or (delay? x) (future? x))
    (get @x k)
    (get x k)))


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
		  (if-let [^AtomicInteger pcounter (get @topic-partition-ref topic)]
		    (mod ^Long (.getAndIncrement pcounter) partition-count)
		    (do 
		     (dosync 
		           (commute topic-partition-ref (fn [x] 
		                                          (assoc x topic (AtomicInteger. 0)))))
		     (select-rr-partition! topic state)))
    (do 
      (error "No topic found " topic " in state map " topic-partition-ref)
      (error "broker-meta " brokers-metadata)
      (throw (RuntimeException. 
             (str "The topic " topic " does not exist, please create it first. See http://kafka.apache.org/documentation.html")))))))

(defn get-broker-from-metadata [topic partition brokers-metadata]
  (-> brokers-metadata (get topic) (get partition)))

(defn random-select-broker [topic brokers-metadata]
  "Select a broker over all the registered topics randomly"
  (-> (map (fn [[k v]] [k v]) brokers-metadata) rand-nth rest rand-nth))

  
(defn select-broker [topic partition brokers-metadata]
  "Try to find the broker in the brokers-metadata by topic and partition,
    if no broker can be found, a random select is done, if still
    no broker is found nil is returned"
  (if-let [broker (get-broker-from-metadata topic partition @brokers-metadata)]
    [partition broker]
    [0 (random-select-broker topic brokers-metadata)]))

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

(defn create-producer-buffer [connector topic partition producer-error-ch {:keys [host port]} {:keys [batch-num-messages queue-buffering-max-ms] :or
                                                                   {batch-num-messages 100 queue-buffering-max-ms 1000} :as conf}]
  "Creates a producer and buffered-chan with a go loop that will read off the buffered chan and send to the producer.
   A map with keys :producer ch-source and buff-ch is returned"
  (let [producer (producer host port conf)
        c (:client producer)
        ch-source (chan 100)
        read-ch (-> producer :client :read-ch)
                               ;ch-source buffer-count timeout-ms buffer-or-n check-f
        buff-ch (buffered-chan ch-source batch-num-messages queue-buffering-max-ms 10 always-false)
        
        handle-send-message-error (fn [e producer conf offset v]
                                    (error e e)
                                    (prn "handle-send-message-error: v " v)
                                    (>!! producer-error-ch {:key-val (str topic ":" partition) :error e :producer {:producer producer ::buff-ch buff-ch}
                                                            :offset offset :v v :topic topic}))]
    
   
    ;if a response from the server (only when ack > 0)
    ; if error-codec > 0 then handle the error
    ; else remove the message from the cache
    (go-seq 
       (fn [v]
         (try
	          (if (instance? ProduceResponse v) ;ProduceResponse [correlation-id topic partition error-code offset])
	             (let [{:keys [correlation-id topic partition offset]} v]
                 ;(debug "produce response " v)
                  (if (> (:error-code v) 0)
			              (if (:send-cache connector)
                       (handle-send-message-error 
			                       (RuntimeException. (str "Response error " (:error-code v))) 
			                          producer conf offset (get-sent-message connector topic partition correlation-id))
                       (error "Message received (even though acks " (:acks conf) " msg " v))) ; TODO send error back to connection and exit
			              (remove-sent-message connector topic partition correlation-id)))
           (catch Exception e (error e e)))) 
       read-ch)
    
    ;;if any error on the tcp client handle error
    (go-seq
      (fn [[e v]]
        (if (fn? v) 
          (handle-send-message-error 
                    (RuntimeException. (str "Client tcp error " e)) 
                    producer conf v -1)
          (error "Cannot react on message " v))
        
        (error e e))  (:error-ch c))
		    
    ;send buffered messages
    ;if any exception handle the error
    (go-seq
        (fn [v]
           (if (> (count v) 0) 
                 (do
                   (try 
                       (send-messages connector producer conf v) 
                       (catch Exception e (handle-send-message-error e producer conf v -1)))))) buff-ch)
    
    {:host host
     :port port
     :producer producer
     :ch-source ch-source
     :buff-ch buff-ch}))
  
(defn add-remove-on-error-listener! [producers-ref topic key-val producer-buffer brokers-metadata]
  "If any error is read on the client error-ch this producer this removed from the producers-ref"
  ;this loop will block and on the first error, close and remove the producer, then exit the loop
  (let [error-ch (-> producer-buffer :producer :client :error-ch)]
	   (go
      (if-let [error-val (<! error-ch)]
       (do
         ;(error (first error-val) (first error-val))
         (prn "removing producer for " key-val)
         (try 
           (shutdown (:producer producer-buffer))
           (catch Exception e (error e e)))
         ;remove from ref
         (dosync 
           (alter producers-ref (fn [m] (dissoc m key-val)))
           
         
         )
        )))))
     
  
(defn find-hashed-connection [broker k producers upper-limit]
  "Finds the value with a key that when hashed and moded against upper-limit is the same for k
   this is an optimization for hashed partitioning where the original keys are stored in the producers-ref
   i.e. topic:partition but the connections are created on (mod (hash k) upper-limit)"
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
             
(defn select-producer-buffer! [connector topic partition {:keys [producers-ref brokers-metadata producer-error-ch conf] :as state}]
  "Select a producer or create one,
   if no broker can be found a RuntimeException is thrown"
  (let [k (str topic ":" partition)]
	  (if-let [producer (get @producers-ref k)]
   	    @producer
		    (deref 
          (get (dosync (alter producers-ref 
                           (fn [x] 
                             (if-let [producer (get @producers-ref k)]
                               x ; return map the producer already exists
                               (let [[partition broker] (select-broker topic partition brokers-metadata)]
	                               (if-let [producer (find-hashed-connection broker k @producers-ref (get conf :producer-connections-max 4))]
	                                 (assoc x k producer) ; found a producer based on hash and broker for the same partition
	                                 (assoc x k (delay ;else create a new producer
		                                            (let [producer-buffer (create-producer-buffer connector topic partition producer-error-ch broker conf)]
			                                             (add-remove-on-error-listener! producers-ref topic k producer-buffer brokers-metadata)
		                                               producer-buffer)))))))))
            k)))))
	     

(defn- send-msg-retry [{:keys [state] :as connector} {:keys [topic] :as msg}]
  "Try sending the message to any of the producers till all of the producers have been trieds"
    (let [partitions (-> state :brokers-metadata (get topic))]
      (if (or (empty? partitions) (= partitions 0))
        (throw (RuntimeException. (str "No topics available for topic " topic))))
      
      (loop [partitions-1 partitions i 0] 
        (if-let [partition (first partitions-1)]
          (let [sent (try 
						            (let [producer-buffer (select-producer-buffer! connector topic partition state)]
                          (prn "retry sending message to " (:host producer-buffer))
						              (send-to-buffer state msg)
						              true)
						            (catch Exception e (do (error e e) false)))]
            (if (not sent)
              (recur (rest partitions) (inc i))))
          (throw (RuntimeException. (str "The message for topic " topic " could not be sent")))))))

(defn send-msg [{:keys [state] :as connector} topic ^bytes bts]
  (if (> (-> state :brokers-metadata deref count) 0)
	  (let [partition (select-rr-partition! topic state)
	        producer-buffer (select-producer-buffer! connector topic partition state)
	        ]
      (try
       (send-to-buffer producer-buffer (message topic partition bts))
       (catch Exception e
         (do (error e e)
           ;here we try all of the producers, if all fail we throw an exception
           (send-msg-retry connector (message topic partition bts))
           
           ))))
         
   (throw (RuntimeException. (str "No brokers available: " connector)))))

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
  (stop-fixdelay (:metadata-fixdelay state))
  
  ;(stop-fixdelay (:retry-cache-ch state))
	(close-retry-cache connector) ;here we get 100% cpu use
  (close-send-cache connector)
 
  (doseq [producer-buffer (deref (:producers-ref state))]
    (close-producer-buffer! producer-buffer))
  )


(defn create-connector [bootstrap-brokers {:keys [acks] :or {acks 0} :as conf}]
  (let [
        metadata-producers (map #(metadata-request-producer (:host %) (:port %) conf) bootstrap-brokers)
        brokers-metadata (ref (get-metadata metadata-producers conf))
        _ (do
            (info ">>>>>>>> brokers-metadata " brokers-metadata)

            (if (empty? @brokers-metadata)
                (throw (RuntimeException. (str "No broker metadata could be found for " bootstrap-brokers)))))
        producer-error-ch (chan 1000)
        producer-ref (ref {})
        send-cache (if (> acks 0) (create-send-cache conf))
        retry-cache (create-retry-cache conf)
        state {:producers-ref producer-ref
               :brokers-metadata brokers-metadata
               :topic-partition-ref (ref {})
               :producer-error-ch producer-error-ch
               :conf conf}
        ;go through each producer, if it does not exist in the metadata, the producer is closed, otherwise we keep it.
        update-producers (fn [metadata m] 
                               (into {} (filter (complement nil?)
                                            (map (fn [[k producer-buffer]]
								                                                    (let [[topic partition] (clojure.string/split k #"\:")
								                                                          host (smart-get producer-buffer :host)]
								                                                      (if (some #(= (:host %) host) (smart-get metadata topic))
								                                                        [k producer-buffer]
								                                                        (do
                                                                          (prn "closing producer " host " topic " topic " prodbuffer " producer-buffer)
								                                                          (close-producer-buffer! producer-buffer)
								                                                          nil))))
                                              m))))
        update-metadata (fn [] 
                          (let [metadata (get-metadata metadata-producers conf)]
	                          (dosync 
                              (alter brokers-metadata (fn [x] metadata))
                              (alter producer-ref (fn [x] (update-producers metadata x)))
                              )))
                           
        metadata-fixdelay 
                  ;start periodic metadata scanning
							    (fixdelay 5000
							              (try
							                (update-metadata)
							                (catch Exception e (error e e))))
                  
        connector {:bootstrap-brokers metadata-producers :send-cache send-cache :retry-cache retry-cache
                   :state (assoc state :metadata-fixdelay metadata-fixdelay) }
        
                  ;;every 10 seconds check for any data in the retry cache and resend the messages 
				retry-cache-ch (fixdelay 5000
										      (try
                            (do
                             (doseq [retry-msg (retry-cache-seq connector)]
                                 (do (prn "Retry messages for " (:topic retry-msg) " " (:key-val retry-msg))
                                   (if (coll? (:v retry-msg))
													           (doseq [{:keys [bts]} (:v retry-msg)]
                                        (send-msg connector (:topic retry-msg) bts))
                                     (error "Invalid retry value " retry-msg))
												              
										                  (delete-from-retry-cache connector (:key-val retry-msg)))))
										         (catch Exception e (error e e))))]
    ;listen to any producer errors, this can be sent from any producer
    ;update metadata, close the producer and write the messages in its buff cache to the 
    (go-seq
      (fn [error-val]
        (try
          (prn "producer error producer-error-ch")
	        (let [{:keys [key-val producer v topic]} error-val]
            ;persist to retry cache
            (error "!!! ERROR write to retry cache : "  key-val " " v " " topic)
            (.printStackTrace (:error error-val))
            
            (if (coll? v) ;only write valid messages to the retry cache
              (write-to-retry-cache connector topic v))
           
		        (update-metadata)
		        (info "removing failed producer " (:broker producer) " and updating metadata")
		        (dosync 
	                 (alter producer-ref (fn [m] (dissoc m key-val))))
          
            ;close producer and cause the buffer to be flushed
            (close-producer-buffer!  producer))
          (catch Exception e (error e e)))) producer-error-ch)
	    
   
          
      
     (assoc connector :retry-cache-ch retry-cache-ch)
    ))
 


