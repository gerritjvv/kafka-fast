(ns kafka-clj.consumer

  (:require
            [group-redis.core :refer [create-group-connector join get-members close reentrant-lock release persistent-set* persistent-get]]
            [clojure.tools.logging :refer [info error]]
            [clj-tcp.client :refer [client write! read! close-all close-client]]
            [kafka-clj.produce :refer [shutdown message]]
            [kafka-clj.fetch :refer [create-fetch-producer create-offset-producer send-offset-request send-fetch read-fetch]]
            [kafka-clj.metadata :refer [get-metadata]]
            [fun-utils.core :refer [buffered-chan]]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :refer [<!! >!! alts!! timeout chan go >! <! close! go-loop]]
            [clj-tuple :refer [tuple]])
  (:import [kafka_clj.fetch Message FetchError]
           [com.codahale.metrics Meter MetricRegistry Timer Histogram]
           [java.util.concurrent Executors ExecutorService Future Callable]
           [io.netty.buffer Unpooled]
           [java.io File DataOutputStream]))

             
 ;------- partition lock and release api

(defonce ^MetricRegistry metrics-registry (MetricRegistry.))

(defn- flatten-broker-partitions [broker-offsets]
  (for [[broker topics] broker-offsets
        [topic partitions] topics
        partition partitions]
    (assoc partition :broker broker :topic topic)))

(defn- get-add-partitions [broker-partitions n]
  "Returns n partitions that are not marked as locked"
  (take n (filter (complement :locked) broker-partitions)))

(defn- get-remove-partitions [broker-partitions n]
  "Returns n partitions that should be removed and that are locked"
  (take n (filter :locked broker-partitions)))

 (defn get-partitions-to-lock [topic broker-offsets members]
   "broker-offsets {broker {topic [{:partition :offset :topic}]}}
    Returns the number of partitions that should be locked"
   ;;TODO the problem is that the partitions that were marked as locked are not locked in broker-offsets, i.e. the locked
   ;;attribute is not kept in the map
   (try
	   (let [broker-partitions (filter #(= (:topic %) topic) (flatten-broker-partitions broker-offsets))
           
	         partition-count (count broker-partitions)
	         locked-partition-count (count (filter :locked broker-partitions))
           ;only count members of the same group consuming the same topic
           member-count (count (filter (fn [m] (some #(= topic %) (-> m :val :sub-groups))) members))
	         e (long (/ partition-count member-count))
	         l (rem partition-count member-count)]
	     
	     ;(info "members " members " partition-count " partition-count " locked-partition-count " locked-partition-count " e " e " l " l )
	     [(if (> e locked-partition-count) (count (get-add-partitions broker-partitions e)) 0)
	      (if (> locked-partition-count e) (count (get-remove-partitions broker-partitions (- locked-partition-count e))) 0)
	      l
	      ])
    (catch Exception e 
      (do 
        (.printStackTrace e)
        (error (str "Error while calculating partitions to lock members: " members " broker-partitions " (filter #(= (:topic %) topic) (flatten-broker-partitions broker-offsets))))
        [0 0 0]))))
 
 
 ;------- end of partition lock and release api

(defn replace-partition [partitions offset partition]
  "partitions = [{:partition :offset ...} ...]
   partition = Long
   offset = Long"
  (let [p1 (first (filter #(= (:partition %) partition) partitions))
        ps (filter #(not (= (:partition %) partition)) partitions)]
    (conj ps (assoc p1 :offset offset))))

            
(defn merge-broker-offsets [curr-state d]
  "D is a collection of messages one per topic partition, that were last consumed from a fetch request,
   state is the broker-offsets {broker {topic [{:partition :offset :topic}]}}
   The function will merge d with state so that state will contain the latest offsets d,
   and then returns the new state
   "
  (reduce (fn [state1 [broker messages]]
          (reduce (fn [state {:keys [topic offset partition]}]
                    ;;find and replace the partition in state format {:host "broker", :port 9092} {"topic" [{:offset 0, :error-code 0, :locked true, :partition 4} {:offset 0, :error-code 0, :locked true, :partition 5}]}}
                    (assoc-in state [broker topic] (replace-partition (-> state (get broker) (get topic)) (inc offset) partition)) 
                    
                    ) state1 messages))
         curr-state d))

(defn- get-latest-offset [k current-offsets resp]
  "Helper function for send-request-and-wait, k is searched in resp, if no entry current-offsets is searched, and if none is found 0 is returned"
  (if-let [o (get resp k)]
    (:offset o)
    (if-let [o (get current-offsets k)]
      (let [l (dec (:offset o))] ;we decrement the current offset, th reason is this is the pinged offset, the last 
                                 ;consumed offset is always (dec pinged-offset)
        (if (> l 0) l 0))
      (throw (RuntimeException. (str "Cannot find " k " in " current-offsets))))))


(defn- write-persister-data [group-conn state]
  "Converts state to [[k val] ... ] and sends to persisent-set*"
  (persistent-set* group-conn (vec state)))
  
(defn get-persister [group-conn conf]
  "Returns an object that have functions p-close p-send"
  (let [{:keys [offset-commit-freq ^Meter m-redis-reads ^Meter m-redis-writes] :or {offset-commit-freq 5000}} conf
        ch (chan 100)]
    
    (go
      (try
	      (loop [t (timeout offset-commit-freq) state {}]
	          (let [[v c] (alts! [ch t])]
	            (if (= c ch)
	              (if (nil? v)
	                  (do (.mark m-redis-writes) (write-persister-data group-conn state))  ;channel is closed
			            (if (= c ch)
			              (recur t (assoc state (clojure.string/join "/" [(:topic v) (:partition v)]) (:offset v)))))
	               ;timeout
	              (do
                  (.mark m-redis-writes)
	                (write-persister-data group-conn state)
	                  (recur (timeout offset-commit-freq)
	                         {})))))
        (catch Exception e (error e e))))
	    
    {:ch ch :p-close #(close! ch) :p-send #(>!! ch %)}))
                         


(defn is-new-msg? [current-offsets resp k v]
  "True if the message has not been seen yet"
  (let [latest-offset (get-latest-offset k current-offsets resp)]
    (or (> (:offset v) latest-offset) (= (:offset v) 0))))

(defn prn-fetch-error [e state msg]
  (error e (str "Internal Error while reading message: e " e))
  (error (str "Internal Error while reading message: state " state " for message " msg)))

(defn read-fetch-message [{:keys [p-send]} current-offsets msg-ch ^Meter m-consume-reads ^Histogram m-message-size v]
  ;read-fetch will return the result of fn which is [resp-vec error-vec]
   (let [
         fetch-res
         (read-fetch (Unpooled/wrappedBuffer ^"[B" v) [{} [] 0]
			     (fn [state msg]
              ;read-fetch will navigate the fetch response calling this function
              ;on each message found, in turn this function will update redis via p-send
              ;and send the message to the message channel (via >!! msg-ch msg)
              (if (coll? state)
		            (let [[resp errors cnt] state]
	               (try
		               (do 
				             (cond
							         (instance? Message msg)
							         (let [k #{(:topic msg) (:partition msg)}]
							           (if (is-new-msg? current-offsets resp k msg)   
				                   (do 
                              ;(if (= cnt 0)
                               ;(write-timestamp msg))
                               (>!! msg-ch msg)
		                           (p-send msg)
                               (.mark m-consume-reads) ;metrics mark
                               (.update m-message-size (count (:bts msg)))
							                 (tuple (assoc resp k msg) errors (inc cnt)))
	                          (tuple resp errors (inc cnt))))
							         (instance? FetchError msg)
							         (do (error "Fetch error: " msg) (tuple resp (conj errors msg) cnt))
							         :else (throw (RuntimeException. (str "The message type " msg " not supported")))))
		               (catch Exception e 
	                  (do (.printStackTrace e)
                        (prn-fetch-error e state msg)
	                      (tuple resp errors cnt))
	                  )))
                  (do (error "State not supported " state)
                      [{} [] 0])
                  )))]
       (if (coll? fetch-res)
          (let [[resp errors cnt] fetch-res]
            ;(info "Messages read " cnt)
	          (tuple (vals resp) errors)) ;[resp-map error-vec]
	       (do
	         (info "No messages consumed " fetch-res)
	         nil))))
  

(defn- get-locked-partitions [topic-offsets]
  "Get the locked partitions"
  (map (fn [[k v]]
         (tuple k
           (filter
             :locked
             v)))
    topic-offsets))
             
(defn send-request-and-wait [producer group-conn topic-offsets msg-ch {:keys [^Histogram m-message-size
                                                                              ^Meter m-consume-reads fetch-timeout] 
                                                                       :or {fetch-timeout 60000} :as conf}]
  "Returns [the messages, and fetch errors], if any error was or timeout was detected the function returns otherwise it waits for a FetchEnd message
   and returns. 
  "
  ;(info "!!!! calling get-locked-partitions with " topic-offsets)
  (let [locked-partitions (get-locked-partitions topic-offsets)]
      (do
	      (info "!!!!!!send fetch " (:broker producer) " "  locked-partitions)
			  (send-fetch producer locked-partitions)
			  
			  (let [
			        persister (get-persister group-conn conf)
			        {:keys [read-ch error-ch]} (:client producer)
			        current-offsets (into {} (for [[topic v] topic-offsets
			                                        msg   v]
			                                      [#{topic (:partition msg)} (assoc msg :topic topic) ]))]
			    
			      (let [[v c] (alts!! [read-ch error-ch (timeout fetch-timeout)])]
			        ;(info "Got message " (count v ) " is read " (= c read-ch) " is error " (= c error-ch))
			        (try
				        (cond 
				          (= c read-ch)
			            ;;read-fetch will navigate and process the fetch response, sending messages to msg-ch
				          (read-fetch-message persister current-offsets msg-ch m-consume-reads m-message-size v)
				          (= c error-ch)
				          (do 
				            (error v v)
				            [[] [{:error v}]])
				          :else
				          (do 
				            (error "timeout reading from " (:broker producer))
				            [[] [{:error (RuntimeException. (str "Timeout while waiting for " (:broker producer)))}]] 
				            ))
			         (finally ((:p-close persister)))))))))


(defn consume-broker [producer group-conn topic-offsets msg-ch conf]
  "Send a request to the broker and waits for a response, error or timeout
   Then threads the call to the route-requests, and returns the result
   Returns [messages, fetch-error]
   "
   (try
      (send-request-and-wait producer group-conn topic-offsets msg-ch conf)
      (catch Exception e (error e e))
      (finally (do
                 ;(info ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> end consume-broker " (:broker producer) " <<<<<<<<<<<<<<<<<<<<<<<<")
                 ))))


(defn transform-offsets [topic offsets-response {:keys [use-earliest] :or {use-earliest true}}]
   "Transforms [{:topic topic :partitions {:partition :error-code :offsets}}]
    to {topic [{:offset offset :partition partition}]}"
   (let [topic-data (first (filter #(= (:topic %) topic) offsets-response))
         partitions (:partitions topic-data)]
     {(:topic topic-data)
            (doall (for [{:keys [partition error-code offsets]} partitions]
                     {:offset (if use-earliest (last offsets) (first offsets))
                      :error-code error-code
                      :locked false
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


(defn get-create-offset-producer [offset-producers-ref broker conf]
  (if-let [producer (get @offset-producers-ref broker)]
    producer
    (get 
      (dosync
	      (alter offset-producers-ref 
	        (fn [m]
	          (if-let [producer (get m broker)]
	            m
	            (assoc m broker (create-offset-producer broker conf))))))
      broker)))
    
  

(defn get-broker-offsets [{:keys [offset-producers]} metadata topics conf]
  "Builds the datastructure {broker {topic [{:offset o :partition p} ...] }}"
   (apply merge-with merge
     (for [topic topics] 
	     (let [topic-data (get metadata topic)
	         by-broker (group-by second (map-indexed vector topic-data))]
	        (into {}
			        (for [[broker v] by-broker]
			          ;here we have data {{:host "localhost", :port 1} [[0 {:host "localhost", :port 1}] [1 {:host "localhost", :port 1}]], {:host "abc", :port 1} [[2 {:host "abc", :port 1}]]}
			          ;doing map first v gives the partitions for a broker
			          (let [offset-producer (get-create-offset-producer offset-producers broker conf)
			                offsets-response (get-offsets offset-producer topic (map first v))]
			            [broker (transform-offsets topic offsets-response conf)])))))))

(defn create-producers [broker-offsets conf]
  "Returns created producers"
    (for [broker (keys broker-offsets)]
          (create-fetch-producer broker conf)
          ))

(defonce ^ExecutorService exec-service (Executors/newCachedThreadPool))

(defn future-f-call [^ExecutorService service ^Callable f]
  (.submit service f))

(defn wait-futures [futures]
  (doall 
    (for [[broker ^Future fu] futures]
      [broker (.get fu)])))

(defn consume-brokers! [producers group-conn broker-offsets msg-ch conf]
  "
   Broker-offsets should be {broker {topic [{:offset o :partition p} ...] }}
   Consume brokers and returns a list of lists that contains the last messages consumed, or -1 -2 where errors are concerned
   the data structure returned is {broker -1|-2|[{:offset o topic: a} {:offset o topic a} ... ] ...}
  "
  (try
    (reduce 
      (fn [[state errors] [broker [msgs msg-errors]]]
         [(merge state {broker msgs}) (if (> (count msg-errors) 0) (apply conj errors msg-errors) errors)])
          [{} []];initial value
          (pmap #(vector (:broker %)  (consume-broker % group-conn (get broker-offsets (:broker %)) msg-ch conf)) 
                    producers))
   (finally
     ;(info ">>>>>>>>>>>>>>>>>>>>> END CONSUME BROKERS!")
     )))

(defn update-broker-offsets [broker-offsets v]
  "
   broker-offsets must be {broker {topic [{:offset o :partition p} ...] }}
   v must be {broker -1|-2|[{:offrokerset o topic: a} {:offset o topic a} ... ] ...}"
   (merge-broker-offsets broker-offsets v))


(defn close-and-reconnect [conn bootstrap-brokers producers topics errors conf]
  
  (let [reconnect (some (fn [x] (not (instance? FetchError x))) errors)]
    
    (if reconnect
		  (doseq [producer producers]
		    (shutdown producer)))  
	  
	
	  (info "close-and-reconnect: " bootstrap-brokers " topic " topics " reconnect " reconnect)
	  (if-let [metadata (get-metadata bootstrap-brokers conf)]
	    (let [broker-offsets (doall (get-broker-offsets conn metadata topics conf))
	          producers (if reconnect (doall (create-producers broker-offsets conf)) producers)]
	      [producers broker-offsets])
	    (throw (RuntimeException. "No metadata from brokers " bootstrap-brokers)))))

(defn- ^long coerce-long [v]
  "Will return a long value, if v is a long its returned as is, if its a number its cast to a long,
   otherwise its converted to a string and Long/parseLong is used"
  (if (instance? Long v) v
    (if (instance? Number v) 
      (long v)
      (if (> (count v) 0)
        (Long/parseLong (str v))
        nil))))

(defn- get-saved-offset [group-conn topic partition {:keys [^Meter m-redis-reads]}]
  "Retreives the offset saved for the topic partition or nil"
  (.mark m-redis-reads)
  (coerce-long 
         (persistent-get group-conn (clojure.string/join "/" [topic partition]))))

(defn- get-rest-of-partitions [broker topic partition state]
  "state should be {broker {topic [{:partition :offset :topic}... ] }}
   This method will return all of the data for a broker topic that does not have :partition == partition"
  (filter #(not (= (:partition %) partition)) (-> state (get broker) (get topic))))


(defn get-partition [broker topic partition state]
  "state should be {broker {topic [{:partition :offset :topic}... ] }}
   This method will return all of the data for a broker topic that does not have :partition == partition"
  (first (filter #(= (:partition %) partition) (-> state (get broker) (get topic)))))


(defn change-partition-lock [group-conn broker-offsets broker topic partition locked? conf]
  "broker-offsets = {broker {topic [{:partition :offset :topic}]}}
   change the locked value of a partition
   returns the modified broker-offsets

   Any records that cannot be locked are removed from the map returned"
  (let [rest-records (get-rest-of-partitions broker topic partition broker-offsets)
           p-record (get-partition broker topic partition broker-offsets)
           saved-offset (get-saved-offset group-conn topic partition conf)
           ]
      
      (if p-record (merge-with merge broker-offsets
                                       {broker {topic 
                                                   (conj rest-records (assoc p-record :locked locked?
		                                                                                        :offset (if saved-offset (inc saved-offset) 
		                                                                                                    (:offset p-record) ) ))
                                                   }})
          (do
            (error "Error no record found : " p-record)
            broker-offsets)
          
        )))

(defn- release-partition [group-conn topic partition conf]
  (if-let [host (get conf :host-name nil)] 
    (release group-conn host (str topic "/" partition))
    (release group-conn (str topic "/" partition))))
    
(defn- lock-partition [group-conn topic partition conf]
  (if-let [host (get conf :host-name nil)]  
    (reentrant-lock group-conn host (str topic "/" partition))
    (reentrant-lock group-conn (str topic "/" partition))))



(defn calculate-locked-offsets [topic group-conn init-broker-offsets conf]
  "broker-offsets have format  {broker {topic [{:partition :offset :topic}]}}
   calculate which offsets should be consumed based on the locks and other members
   returns the broker-offsets marked as locked or not as locked."
  ;(info "host " (get conf :host-name))
  
  (let [
        broker-partitions (filter #(= (:topic %) topic) (flatten-broker-partitions init-broker-offsets))
        [locked-n remove-n l] (get-partitions-to-lock topic init-broker-offsets (get-members group-conn))
        
          broker-offsets2 (loop [broker-offsets1 init-broker-offsets locked-i locked-n  remove-i remove-n l-i l partitions broker-partitions]
                            (if-let [record (first partitions)]
                              (let [{:keys [topic broker partition locked]} record]
                                 (cond 
                                   (and locked (> remove-i 0))
                                   (do 
                                     (info ">>>>>>>>>>>>>>>>>>>>>>>>>>> calling release on " topic "/" partition)
                                     (release-partition group-conn topic partition conf)
                                     (recur (change-partition-lock group-conn broker-offsets1 broker topic partition false conf)
                                            locked-i (dec remove-i) l-i (rest partitions)))
                                    (and (not locked) (or (> locked-i 0) (> l-i 0)))
                                         (do
                                           (info "======== calling lock on =========== " (str topic "/" partition))
                                           (if (lock-partition group-conn topic partition conf)
                                              ;here we know that we have a lock
                                              (do 
                                                    ;here the locks are created properly
							                                (recur (change-partition-lock group-conn
								                                                                 broker-offsets1 
																	                                               broker topic partition 
																	                                               true
								                                                                 conf)
                                                     (dec locked-i) remove-i (if (> locked-i 0) l-i (dec l-i)) (rest partitions)))
                                               (do 
                                                 (info ">>>>>>>>>>>>>>>>>>>>>>>>>>> calling release on " topic "/" partition)
                                                 (release-partition group-conn topic partition conf)
                                                 (recur (change-partition-lock group-conn broker-offsets1 broker topic partition false conf)
                                                   locked-i (dec remove-i) l-i (rest partitions)))))
                                                 
                                              
                                       
                                    :else
                                    (recur broker-offsets1 locked-i remove-i l-i (rest partitions))))
                                           
                                 broker-offsets1))]
        
            
	          broker-offsets2
	          
	        ))
    
  
(defn persist-error-offsets [group-conn broker-offsets errors conf]
  (let [{:keys [p-close p-send]} (get-persister group-conn conf)
        offsets (flatten-broker-partitions broker-offsets)]
    (info "Updating offsets for errors " errors " using offsets " offsets)
	  (doseq [{:keys [topic partition]} errors]
     (if (and topic partition)
	     (if-let [record (first (filter #(and (= (:topic %) topic) (= (:partition %) partition)) offsets))]
	       (do
	         (info "updating " topic " " partition " to " record)
	         (p-send {:topic topic :partition partition :offset (:offset record)}))
	       (info "The record " topic " " partition " cannot be found"))))
    (p-close)))

     
(defn consume-producers! [conn 
                          bootstrap-brokers
                          group-conn
                          producers topics broker-offsets-p msg-ch {:keys [^Timer m-consume-cycle fetch-poll-ms] 
                                                                    :or {fetch-poll-ms 10000} :as conf}]
  "Consume from the current offsets,
   if any error the producers are closed and a reconnect is done, and consumption is tried again
   otherwise the broker-offsets are updated and the next fetch is done"
  
  (loop [producers producers broker-offsets1 broker-offsets-p]
      ;v is [broker data]
       ;(info "11 broker-offsets1 " broker-offsets1)
	      (let [ broker-offsets2 (apply merge-with merge 
	                                            (for [topic topics] (calculate-locked-offsets topic group-conn 
                                                                   ;;we need to remove all other topics from the offset map
                                                                   (into {} (for [[broker topics] broker-offsets1  
                                                                                  [topic1 offsets] topics :when (= topic1 topic) ] [broker {topic offsets}]))
                                                                   conf)))
               timer-ctx (.time m-consume-cycle)
               q (consume-brokers! producers group-conn broker-offsets2 msg-ch conf)]
         
         
	       (let [[v errors] q]
			    (if (and (not (nil? errors)) (> (count errors) 0))
			      (do
			         (error "Error close and reconnect: " errors)
            
			         (let [[producers broker-offsets] (close-and-reconnect conn bootstrap-brokers producers topics errors conf)]
	                (info "Got new consumers " (map :broker producers))
	                (persist-error-offsets group-conn broker-offsets errors conf)
                  (.stop timer-ctx)
			            (recur producers broker-offsets)))
			      (do
               (if (< (reduce #(+ %1 (count %2)  ) 0 (vals v)) 1) ; if we were reading data, no need to pause
	               (do (info "sleep: " fetch-poll-ms) (<!! (timeout fetch-poll-ms))))
	             
               (.stop timer-ctx)
               (recur producers (update-broker-offsets broker-offsets2 v)))))
	
			      )))

(defn consume [conn bootstrap-brokers group-conn msg-ch topics conf]
  "Entry point for topic consumption,
   The cluster metadata is requested from the bootstrap-brokers, the topic offsets are sorted per broker.
   For each broker a producer is created that will control the sending and reading from the broker,
   then consume-producers is called in the background that will reconnect if needed,
   the method returns with {:msg-ch and :shutdown (fn []) }, shutdown should be called to stop all consumption for this topic"
  (if-let [metadata (get-metadata bootstrap-brokers {})]
    (let[broker-offsets (doall (get-broker-offsets conn metadata topics conf))
         producers (doall (create-producers broker-offsets conf))
         t (future (try
                     (consume-producers! conn bootstrap-brokers group-conn producers topics broker-offsets msg-ch conf)
                     (catch Exception e (error e e))))]
      {:msg-ch msg-ch :shutdown (fn [] (future-cancel t))}
      )
     (throw (Exception. (str "No metadata from brokers " bootstrap-brokers)))))

(defn create-metrics []
       {:m-consume-reads (.meter metrics-registry (str "kafka-consumer.consume-#" (System/nanoTime)))
        :m-redis-reads (.meter metrics-registry (str "kafka-consumer.redis-reads-#" (System/nanoTime)))
        :m-redis-writes (.meter metrics-registry (str "kafka-consumer.redis-writes-#" (System/nanoTime)))
        :m-message-size (.histogram metrics-registry (str "kafka-consumer.msg-size-#" (System/nanoTime)))
        :m-consume-cycle (.timer metrics-registry (str "kafka-consume.cycle-#" (System/nanoTime)))})
        
(defn consumer [bootstrap-brokers topics conf]
 "Creates a consumer and starts consumption
  Group management:
      The join is done using either :host-name if its defined in conf, otherwise join is done as (join c) using the host name.
  "
  (info "Connecting to redis using " (get conf :redis-conf {:heart-beat-freq 10}))
  (let [
        offset-producers (ref {})
        metrics (create-metrics)
        msg-ch (chan 100)
        msg-buff (buffered-chan msg-ch 1000 1000 1000)
        redis-conf (get conf :redis-conf {:heart-beat-freq 10})
        group-conn (let [c (create-group-connector (get redis-conf :redis-host "localhost") (assoc redis-conf :sub-groups topics))
                         host-name (get conf :host-name nil) ]
                     (if (nil? host-name)
                          (join c)
                          (join c host-name))
                     c)
        consumers [(consume {:offset-producers offset-producers} bootstrap-brokers group-conn msg-ch (into #{} topics) (merge conf metrics))]
       
        shutdown (fn []
                   (close group-conn)
                   (doseq [[_ producer] @offset-producers]
                           (shutdown producer))
                   (doseq [c consumers]
                     ((:shutdown c))))]
    
    {:shutdown shutdown :message-ch msg-buff :offset-producers offset-producers :group-conn group-conn :metrics metrics :consumers consumers}))


(defn close-consumer [{:keys [shutdown]}]
  (shutdown))

(defn shutdown-consumer [{:keys [shutdown]}]
  "Shutsdown a consumer"
  (shutdown))
  ;(.shutdown exec-service)
  ;(.shutdownNow exec-service)
  
 (defn read-msg
   ([{:keys [message-ch]}]
       (<!! message-ch))
   ([{:keys [message-ch]} timeout-ms]
   (first (alts!! [message-ch (timeout timeout-ms)]))))

 
