(ns kafka-clj.metadata
  (:require 
            [clj-tuple :refer [tuple]]
            [kafka-clj.produce :refer [metadata-request-producer send-metadata-request shutdown]]
            [fun-utils.core :refer [fixdelay]]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.async :refer [go <! <!! >!! alts!! timeout thread]])
  (:import [java.nio ByteBuffer]
           [clj_tcp.client Poison Reconnected]))

"Keeps track of the metadata
 "

(defn convert-metadata-response [resp]
  ;; transform the resp into a map
  ;; {topic-name [{:host host :port port} ] }
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
																																	                         :when (= (:partition-error-code partition) 0) 
																			                                                     :let [broker (get brokers-by-node (:leader partition))]
						                                                                               ]
																																	                     [(:partition-id partition) broker])))))])))
						                    
                  
                
        ]
    m))

(defn send-update-metadata [producer conf]
  (try
      (send-metadata-request producer conf)
      (catch Exception e (error e e))))

(defn get-broker-metadata [metadata-producer {:keys [metadata-timeout] :or {metadata-timeout 60000} :as conf}]
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

(defn- exception-if-blacklisted
  "blacklisted-producers keys are (str host ':' port)
   Throws a runtime exception if  host port combindation is found"
  [{:keys [host port] :as producer} blacklisted-producers]
  (if (get blacklisted-producers (str host ":" port))
    (throw (RuntimeException. (str "Black listed producer: " host ":" port)))
    producer))

(defn- _get-metadata [metadata-producers blacklisted-metadata-producers-ref conf]
  "Iterate through the brokers, and the first one that returns a metadata response is used"
     (if-let [metadata-producer (exception-if-blacklisted (first metadata-producers) @blacklisted-metadata-producers-ref)]
       (try
         (do
           (get-broker-metadata metadata-producer conf))
         (catch Exception e (do (.printStackTrace e)
                                (prn "error " e)
                                  (dosync (alter blacklisted-metadata-producers-ref (fn [x] (assoc x (str (:host metadata-producer) (:port metadata-producer)) true))))
                                  (if (rest metadata-producers) (_get-metadata (rest metadata-producers) blacklisted-metadata-producers-ref conf)
                                  (error e e)))))
       (throw (RuntimeException. (str "All bootstrap brokers return errors when queried for metadata, no metadata found")))))

(defn get-metadata [metadata-producers conf & {:keys [blacklisted-metadata-producers-ref retry retry-i] :or {retry 3 retry-i 0 blacklisted-metadata-producers-ref (ref {})}}]
  (if (empty? metadata-producers)
    (throw (RuntimeException. (str "At least one meta data producer must be defined")))
    (let [meta (_get-metadata metadata-producers blacklisted-metadata-producers-ref conf)]
      (if (empty? meta)
        (if (< retry-i retry)
          (do
            (warn "retry")
            (get-metadata metadata-producers conf :retry retry :retry-i (inc retry-i)) :blacklisted-metadata-producers-ref blacklisted-metadata-producers-ref)
          (throw (RuntimeException. (str "Unabled to get metadata from brokers meta " meta " producers " metadata-producers " conf " conf))))
        (do
          meta)))))

     
     
     
