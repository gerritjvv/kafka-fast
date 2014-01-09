(ns kafka-clj.consumer
  
  (:require 
            [clojure.tools.logging :refer [info error]]
            [clj-tcp.client :refer [client write! read! close-all]] 
            [kafka-clj.produce :refer [shutdown message]]
            [kafka-clj.fetch :refer [create-fetch-producer create-offset-producer send-offset-request send-fetch]]
            [kafka-clj.metadata :refer [get-metadata]]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :refer [<!! >!! alts!! timeout chan]]))




(defn send-request-and-wait [producer topic-offsets {:keys [fetch-timeout] :or {fetch-timeout 10000}}]
  "Returns the messages, or -1 error -2 timeout"
  ;convert {topic ({:offset 135111084, :error-code 0, :partition 2} {:offset 137539746, :error-code 0, :partition 5})}
  ;to [[topic [{:partition 0} {:partition 1}...]] ... ]
  (send-fetch producer (map (fn [[k v]] [k v]) topic-offsets))
  
  (let [{:keys [read-ch error-ch]} (:client producer)
        [v c] (alts!! [read-ch error-ch (timeout fetch-timeout)])]
    (if v
      (if (= c read-ch)
        v
        (do (error "Error while requesting data from " producer " for topics " (keys topic-offsets)) -1))
        (do (error "Timeout while requesting data from " producer " for topics " (keys topic-offsets)) -2))))


(defn route-requests [{:keys [topics] :as msgs} msg-ch]
  "Returns a collection of the last messages sent one for each topic partition combination
   or of msgs is not a collection the msgs is returned as is"
  (if (coll? msgs)  ;route messages and return a list of the last messages per topic partition sent
    (do
      (info "route-requests -------->>>>>>>>> " msgs)
      ;send every message in {:correlation-id 11, :topics 
      ;([raw-requests-adx ({:partition 0, :error-code 0, :high-water-mark-offset 348700561, 
      ;:messages [{:offset 139305751, :message-size 1522, 
      ;:message [{:crc 1402969336, :key #<byte[] [B@73b9f690>, :bts #<byte[] [B@36bb6683>, :crc2 1402969336}]} {:offset 139305752, :message-size 1515, :message [{:crc 1270530475, :key #<byte[] [B@43604329>, :bts #<byte[] [B@7471451>, :crc2 1270530475}]}
      
      [:a 1]
      ;for each topic return the message with the largest offset
      ;we use doall so that the messages (except the last ones) are gc'ed
      ;(doall (for [[topic v] (group-by :topic msgs) :let [o (last (sort-by :offset v))] ] o))
      )
    
    msgs)) ;else return the msgs that is -1 or -2 depending on the error
  
  

(defn consume-broker [producer topic-offsets msg-ch conf]
  "Send a request to the broker and waits for a response, error or timeout
   Then threads the call to the route-requests, and returns the result
   The result returned can be either, the last messages consumed per topic and partition,
   or -1 for error and -2 for timeout
   "
  (route-requests (send-request-and-wait producer topic-offsets conf) msg-ch))

(defn transform-offsets [topic offsets-response {:keys [use-earliest] :or {use-earliest false}}]
   "Transforms [{:topic topic :partitions {:partition :error-code :offsets}}]
    to {topic [{:offset offset :partition partition}]}"
   (let [topic-data (first (filter #(= (:topic %) topic) offsets-response))
         partitions (:partitions topic-data)]
     {(:topic topic-data)
            (doall (for [{:keys [partition error-code offsets]} partitions]
                     {:offset (if use-earliest (last offsets) (first offsets))
                      :error-code error-code
                      :partition partition}))}))
     
(defn get-offsets [offset-producer topic partitions]
  "returns [{:topic topic :partitions {:partition :error-code :offsets}}]"
  ;we should send format [[topic [{:partition 0} {:partition 1}...]] ... ]
   (send-offset-request offset-producer [[topic (map (fn [x] {:partition x}) partitions)]] )
   (let [{:keys [offset-timeout] :or {offset-timeout 10000}} (:conf offset-producer)
         {:keys [read-ch error-ch]} (:client offset-producer)
         
         [v c] (alts!! [read-ch error-ch (timeout offset-timeout)])
         ]
     (if v
       (if (= c read-ch)
         v
         (throw (RuntimeException. (str "Error reading offsets from " offset-producer " for topic " topic " error: " v))))
       (throw (RuntimeException. (str "Timeout while reading offsets from " offset-producer " for topic " topic))))))

(defn get-broker-offsets [metadata topic conf]
  "Builds the datastructure {broker {topic [{:offset o :partition p} ...] }}"
   (let [topic-data (get metadata topic)
         by-broker (group-by second (map-indexed vector topic-data))]
        (into {}
		        (for [[broker v] by-broker] 
		          ;here we have data {{:host "localhost", :port 1} [[0 {:host "localhost", :port 1}] [1 {:host "localhost", :port 1}]], {:host "abc", :port 1} [[2 {:host "abc", :port 1}]]}
		          ;doing map first v gives the partitions for a broker
		          (let [offset-producer (create-offset-producer broker conf)
		                offsets-response (get-offsets offset-producer topic (map first v))]
		            (shutdown offset-producer)
		            [broker (transform-offsets topic offsets-response conf)])))))
		        
(defn create-producers [broker-offsets conf]
  "Returns created producers"
    (for [broker (keys broker-offsets)]
          (create-fetch-producer broker conf)
          ))


(defn consume-brokers! [producers broker-offsets msg-ch conf]
  "
   Broker-offsets should be {broker {topic [{:offset o :partition p} ...] }}
   Consume brokers and returns a list of lists that contains the last messages consumed, or -1 -2 where errors are concerned
   the data structure returned is {broker -1|-2|[{:offset o topic: a} {:offset o topic a} ... ] ...}
  "
  (doall (into {} (pmap #([%  (consume-broker % (get broker-offsets (:broker %)) msg-ch conf)]) producers))))

(defn broker-error? [v]
  (info "has broker error? " v)
  false
  )

(defn update-broker-offsets [broker-offsets v]
  "
   broker-offsets must be {broker {topic [{:offset o :partition p} ...] }}
   v must be {broker -1|-2|[{:offset o topic: a} {:offset o topic a} ... ] ...}"
  
  broker-offsets
  )


(defn close-and-reconnect [bootstrap-brokers topic producers conf]
  (doseq [producer producers]
    (shutdown producer))
  
  (if-let [metadata (get-metadata bootstrap-brokers topic)]
    (let [broker-offsets (doall (get-broker-offsets metadata topic conf))
          producers (doall (create-producers broker-offsets conf))]
      [producers broker-offsets])
    (throw (RuntimeException. "No metadata from brokers " bootstrap-brokers))))

(defn consume-producers! [bootstrap-brokers producers topic broker-offsets msg-ch conf]
  "Consume from the current offsets, 
   if any error the producers are closed and a reconnect is done, and consumption is tried again
   otherwise the broker-offsets are updated and the next fetch is done"
  (let [v (consume-brokers! producers broker-offsets msg-ch conf)]
    (if (broker-error? v)
      (let [[producers broker-offsets] (close-and-reconnect bootstrap-brokers producers topic conf)]
        (recur bootstrap-brokers producers topic broker-offsets msg-ch conf))
         ;(recur bootstrap-brokers producers topic (update-broker-offsets broker-offsets v) msg-ch conf)
      (info ">>>>>>>>=============== update-broker-offsets: " v)
      )))
        
(defn consume [bootstrap-brokers msg-ch topic conf]
  "Entry point for topic consumption,
   The cluster metadata is requested from the bootstrap-brokers, the topic offsets are sorted per broker.
   For each broker a producer is created that will control the sending and reading from the broker,
   then consume-producers is called in the background that will reconnect if needed,
   the method returns with {:msg-ch and :shutdown (fn []) }, shutdown should be called to stop all consumption for this topic"
  (if-let [metadata (get-metadata bootstrap-brokers {})]
    (let[broker-offsets (doall (get-broker-offsets metadata topic conf))
         producers (doall (create-producers broker-offsets conf))
         t (future (try 
                     (consume-producers! bootstrap-brokers producers topic broker-offsets msg-ch conf)
                     (catch Exception e (error e e))))]
      {:msg-ch msg-ch :shutdown (fn [] (future-cancel t))}
      )
     (throw (Exception. (str "No metadata from brokers " bootstrap-brokers)))))
        
(defn consumer [bootstrap-brokers topics conf]
 "Creates a consumer and starts consumption"  
  (let [msg-ch (chan)
        consumers (doall
                       (for [topic (into #{} topics)]
                         (consume bootstrap-brokers msg-ch topic conf)))
        
        shutdown (fn [] 
                   (doseq [c consumers]
                     ((:shutdown c))))]
    {:shutdown shutdown :message-ch msg-ch}))

(defn shutdown-consumer [{:keys [shutdown]}]
  "Shutsdown a consumer"
  (shutdown))

 (defn read-msg 
   ([{:keys [message-ch]}]
       (<!! message-ch))
   ([{:keys [message-ch]} timeout-ms]
   (first (alts!! [message-ch (timeout timeout-ms)]))))
 
