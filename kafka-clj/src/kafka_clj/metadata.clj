(ns kafka-clj.metadata
  (:require 
            [clj-tuple :refer [tuple]]
            [kafka-clj.produce :refer [metadata-request-producer send-metadata-request shutdown]]
            [fun-utils.core :refer [fixdelay]]
            [clojure.tools.logging :refer [info error]]
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

(defn get-broker-metadata [broker {:keys [metadata-timeout] :or {metadata-timeout 10000} :as conf}]
   "
   Creates a metadata-request-producer, sends a metadata request to the broker and waits for a result,
   if no result in $metadata-timeout or an error an exception is thrown, otherwise the result of
   (convert-metadata-response resp) is returned.
   "
   (let [{:keys [host port]} broker
         producer (metadata-request-producer host port conf)
         read-ch  (-> producer :client :read-ch)
         error-ch (-> producer :client :error-ch)]
	    (try
          (do 
            (send-update-metadata producer conf)
	          ;wait for response or timeout
            (prn "Wait for timeout ")
	          (let [[v c] (alts!! [read-ch error-ch (timeout metadata-timeout)])]
	             (if v
	               (if (= c read-ch) (convert-metadata-response v)
	                 (throw (Exception. (str "Error reading metadata from producer " broker  " error " v))))
	               (throw (Exception. (str "timeout reading from producer " broker ))))))
          (finally 
            (shutdown producer)))))

           
(defn get-metadata [brokers conf]
  "Iterate through the brokers, and the first one that returns a metadata response is used"
     (if-let [broker (first brokers)]
       (try
         (get-broker-metadata broker conf)
         (catch Exception e (do (error "error " e)
                                  (if (rest brokers) (get-metadata (rest brokers) conf)
                                  (error e e)))))))


     
     
     