(ns kafka-clj.client
  (:gen-class)
  (:require [fun-utils.core :refer [star-channel buffered-chan]]
            [kafka-clj.produce :refer [producer send-messages message shutdown]]
            [kafka-clj.metadata :refer [track-broker-partitions]]
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
  (if-let [^AtomicInteger pcounter (get @topic-partition-ref topic)]
    (mod (.getAndIncrement pcounter) (get-partition-count topic @brokers-metadata))
    (do 
     (dosync 
           (commute topic-partition-ref (fn [x] 
                                          (assoc x topic (AtomicInteger. 0)))))
     (select-rr-partition! topic state))
    
    ))

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

(defn create-producer-buffer [topic partition {:keys [host port]} {:keys [batch-num-messages queue-buffering-max-ms] :or 
                                                                   {batch-num-messages 100 queue-buffering-max-ms 1000}}]
  "Creates a producer and buffered-chan with a go loop that will read off the buffered chan and send to the producer.
   A map with keys :producer ch-source and buff-ch is returned"
  (let [producer (producer host port)
        ch-source (chan 100)
        buff-ch (buffered-chan ch-source batch-num-messages queue-buffering-max-ms 10)]
    (go-loop []
        (if-let [v (<! buff-ch)]
             (do
               (if (> (count v) 0) 
                 (do
                   ;(prn "Sending buffered messages " v)
                   (try (send-messages producer {} v) (catch Exception e (.printStackTrace e)))))
                (recur))))
    
    {:producer producer
     :ch-source ch-source
     :buff-ch buff-ch}))
  
(defn select-producer-buffer! [topic partition {:keys [producers-ref brokers-metadata conf] :as state}]
  "Select a producer or create one,
   if no broker can be found a RuntimeException is thrown"
  (let [k (str topic ":" partition)]
	  (if-let [producer (get @producers-ref k)]
   	    producer
		    (if-let [[partition broker] (select-broker topic partition brokers-metadata)]
	           (let [producer-buffer (create-producer-buffer topic partition broker conf)]
	             (dosync (alter producers-ref (fn [x] (assoc x k producer-buffer))))
               producer-buffer)
	           (throw (RuntimeException. "No brokers could be found in the broker metadata"))))))

	     

(defn send-msg [connector topic ^bytes bts]
  (let [state (:state connector)
        partition (select-rr-partition! topic state)
        producer-buffer (select-producer-buffer! topic partition state)
        ]
    (send-to-buffer producer-buffer (message topic partition bts))))

(defn close [{:keys [producers-ref]}]
  "Close all producers and channels created for the connected"
  (doseq [{:keys [producer ch-source]} producers-ref]
    (async/close! ch-source)
    (shutdown producer)))
   
(defn create-connector [bootstrap-brokers conf]
  (let [brokers-metadata (ref {})
        state {:producers-ref (ref {})
               :brokers-metadata brokers-metadata
               :topic-partition-ref (ref {})
               :conf {}}]
    ;start metadata scanning
    (track-broker-partitions bootstrap-brokers brokers-metadata 5000 conf)
    ;block till some data appears in the brokers-metadata
    (loop [i 20]
      (if (>= 0 (count @brokers-metadata))
        (if (> i 0)
            (do 
              (Thread/sleep 500)
              (recur (dec i)))
            (throw (RuntimeException. "No metadata for brokers found")))))
         
    
    {:bootstrap-brokers bootstrap-brokers
     :state state}
    ))
 

(defn -main [& args]
  (let [[options _ _] (reply.main/parse-args args)]
     (reply.main/launch-nrepl options)))


