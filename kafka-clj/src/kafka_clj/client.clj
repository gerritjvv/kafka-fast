(ns kafka-clj.client
  (:gen-class)
  (:require [fun-utils.core :refer [star-channel buffered-chan fixdelay apply-get-create stop-fixdelay]]
            [kafka-clj.produce :refer [producer send-messages message shutdown]]
            [kafka-clj.metadata :refer [get-metadata]]
            [clojure.tools.logging :refer [error info]]
            [reply.main]
            [clojure.core.async :refer [chan >! >!! go-loop] :as async])
  (:import [java.util.concurrent.atomic AtomicInteger]))


(defn- get-partition-count [topic brokers-metadata]
  (count (get brokers-metadata topic)))


(defn select-rr-partition! [topic {:keys [topic-partition-ref brokers-metadata] :as state}]
  "If a counter does not exist in the topic-partition-ref it will be created
   and set using commute, the return result is an increment on the topic counter
   This operation is fast and is benchmarked at over 2 million ops per second
   usage: 
    (def state {:topic-partition-ref (ref {})})
    (select-rr-partition! b {:topic-partition-ref state})
   "
  (let [partition-count (get-partition-count topic @brokers-metadata)]
   (if (> partition-count 0)	    
		  (if-let [^AtomicInteger pcounter (get @topic-partition-ref topic)]
		    (mod (.getAndIncrement pcounter) partition-count)
		    (do 
		     (dosync 
		           (commute topic-partition-ref (fn [x] 
		                                          (assoc x topic (AtomicInteger. 0)))))
		     (select-rr-partition! topic state)))
    (throw (RuntimeException. (str "The topic " topic " does not exist, please create it first. See http://kafka.apache.org/documentation.html"))))))

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

(defn create-producer-buffer [topic partition producer-error-ch {:keys [host port]} {:keys [batch-num-messages queue-buffering-max-ms] :or 
                                                                   {batch-num-messages 100 queue-buffering-max-ms 1000} :as conf}]
  "Creates a producer and buffered-chan with a go loop that will read off the buffered chan and send to the producer.
   A map with keys :producer ch-source and buff-ch is returned"
  (let [producer (producer host port conf)
        ch-source (chan 100)
        read-ch (-> producer :client :read-ch)
        buff-ch (buffered-chan ch-source batch-num-messages queue-buffering-max-ms 10)
        handle-send-message-error (fn [e producer conf v]
                                    (error e e)
                                    (>!! producer-error-ch {:key-val (str topic ":" partition) :error e :producer producer :v v :topic topic})
                                    )]
    
    ;TODO in this loop we can handle produce responses
    ;check and tests for any errors
    (go-loop []
             (if-let [v (<! read-ch)]
               (if (> (:error-code v) 0)
                 (error (str "Error producing " v)))))
    
    (go-loop []
        (if-let [v (<! buff-ch)]
             (do
               (if (> (count v) 0) 
                 (do
                   ;(prn "Sending buffered messages " v)
                   (try 
                       (send-messages producer conf v) 
                       (catch Exception e (handle-send-message-error e producer conf v)))))
                (recur))))
    
    {:host host
     :port port
     :producer producer
     :ch-source ch-source
     :buff-ch buff-ch}))
  
(defn add-remove-on-error-listener! [producers-ref topic key-val producer-buffer brokers-metadata]
  "If any error is read on the client error-ch this producer this removed from the producers-ref"
  ;this loop will block and on the first error, close and remove the producer, then exit the loop
  (let [error-ch (-> producer-buffer :producer :client :error-ch)]
	   (go-loop []
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
     
  
(defn select-producer-buffer! [topic partition {:keys [producers-ref brokers-metadata producer-error-ch conf] :as state}]
  "Select a producer or create one,
   if no broker can be found a RuntimeException is thrown"
  (let [k (str topic ":" partition)]
	  (if-let [producer (get @producers-ref k)]
   	    producer
		    (if-let [[partition broker] (select-broker topic partition brokers-metadata)]
	           (let [producer-buffer (do (prn "Creating producer for " topic) 
                                    (create-producer-buffer topic partition producer-error-ch broker conf))]
	              (add-remove-on-error-listener! producers-ref topic k producer-buffer brokers-metadata)
                (dosync (alter producers-ref (fn [x] (assoc x k producer-buffer))))
               producer-buffer)
	           (throw (RuntimeException. "No brokers could be found in the broker metadata"))))))

	     

(defn- send-msg-retry [state {:keys [topic] :as msg}]
  "Try sending the message to any of the producers till all of the producers have been trieds"
    (let [partitions (-> state :brokers-metadata (get topic))]
      (if (or (empty? partitions) (= partitions 0))
        (throw (RuntimeException. (str "No topics available for topic " topic))))
      
      (loop [partitions-1 partitions i 0] 
        (if-let [partition (first partitions-1)]
          (let [sent (try 
						            (let [producer-buffer (select-producer-buffer! topic state)]
                          (prn "retry sending message to " (:host producer-buffer))
						              (send-to-buffer state msg)
						              true)
						            (catch Exception e (do (error e e) false)))]
            (if (not sent)
              (recur (rest partitions) (inc i))))
          (throw (RuntimeException. (str "The message for topic " topic " could not be sent")))))))

(defn send-msg [{:keys [state]} topic ^bytes bts]
  (if (> (-> state :brokers-metadata deref count) 0)
	  (let [partition (select-rr-partition! topic state)
	        producer-buffer (select-producer-buffer! topic partition state)
	        ]
	    (try
       (send-to-buffer producer-buffer (message topic partition bts))
       (catch Exception e
         (do (error e e)
           ;here we try all of the producers, if all fail we throw an exception
           (send-msg-retry state (message topic partition bts))
           
           ))))
         
   (throw (RuntimeException. (str "No brokers available")))))

(defn close-producer-buffer! [{:keys [producer]}]
  (try
		    (do 
	        (shutdown producer))
      (catch Exception e (error e (str "Error while shutdown " producer)))))

(defn close [{:keys [state]}]
  "Close all producers and channels created for the connected"
  (stop-fixdelay (:metadata-fixdelay state))
  
  (doseq [producer-buffer (deref (:producers-ref state))]
    (close-producer-buffer! producer-buffer)))


(defn create-connector [bootstrap-brokers conf]
  (let [brokers-metadata (ref (get-metadata bootstrap-brokers conf))
        producer-error-ch (chan)
        producer-ref (ref {})
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
								                                                          host (:host producer-buffer)]
								                                                      (if (some #(= (:host %) host) (get metadata topic))
								                                                        [k producer-buffer]
								                                                        (do
                                                                          (prn "closing producer " host " topic " topic)
								                                                          (close-producer-buffer! producer-buffer)
								                                                          nil))))
                                              m))))
        update-metadata (fn [] 
                          (let [metadata (get-metadata bootstrap-brokers conf)]
	                          (dosync 
                              (alter brokers-metadata (fn [x] metadata))
                              (alter producer-ref (fn [x] (update-producers metadata x)))
                              )))
                           
        metadata-fixdelay 
                  ;start periodic metadata scanning
							    (fixdelay 5000
							              (try
							                (update-metadata)
							                (catch Exception e (error e e))))]
    
    (go-loop []
      (when-let [error-val (<! producer-error-ch)]
        (try
	        (let [{:keys [key-val producer v topic]} error-val]
		        (update-metadata)
		        (info "removing failed producer " (:broker producer) " and updating metadata")
		        (dosync 
	                 (alter producer-ref (fn [m] (dissoc m key-val))))
            ;TODO retry message sent
	          (shutdown producer))
          (catch Exception e (error e e)))))
	        
        
    
    {:bootstrap-brokers bootstrap-brokers
     :state (assoc state :metadata-fixdelay metadata-fixdelay) }
    ))
 

(defn -main [& args]
  (let [[options _ _] (reply.main/parse-args args)]
     (reply.main/launch-nrepl options)))


