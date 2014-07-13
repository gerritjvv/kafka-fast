(ns kafka-clj.response
  (:require [clojure.tools.logging :refer [info error]]
            [kafka-clj.buff-utils :refer [read-short-string]])
  (:import 
           [io.netty.handler.codec ByteToMessageDecoder ReplayingDecoder]
           [io.netty.buffer ByteBuf]
           [java.util List]
           [kafka_clj.util SafeReplayingDecoder ProduceStates]
           [java.util.concurrent.atomic AtomicInteger AtomicReference]
           ))

(defrecord ResponseEnd [])
(defrecord ProduceResponse [correlation-id topic partition error-code offset])

(defn error? [error-code]
  (> error-code 0))

(defonce ^:constant error-mapping {
	                        0 "NoError"
	                        -1 "Unknown"
	                        1 "OffsetOutOfRange"
	                        2 "InvalidMessage"
	                        3 "UnknownTopicOrPartition"
	                        4 "InvalidMessageSize"
	                        5 "LeaderNotAvailable"
	                        6 "NotLeaderForPartition"
	                        7 "RequestTimedOut"
	                        8 "BrokerNotAvailable"
	                        9 "ReplicaNotAvailable"
	                        10 "MessageSizeTooLarge"
	                        11 "StaleControllerEpochCode"
	                        12 "OffsetMetadataTooLargeCode"
	                        })

(defn read-metadata-response [^ByteBuf in]
  (let [size (.readInt in)                     ;request size
        correlation-id (.readInt in)           ;correlation id
        broker-count (.readInt in)             ;broker array len
        brokers   (doall 
	                  (for [i (range broker-count)]
	                    {:node-id (.readInt in)
	                     :host (read-short-string in)
	                     :port (.readInt in)}))
        topic-metadata-count (.readInt in)
        topics (doall
                 (for [i (range topic-metadata-count)]
                   (let [error-code (.readShort in) 
                         topic (read-short-string in)]
	                   (if topic
		                   {:error-code error-code
		                    :topic topic
		                    :partitions
		                                (let [partition-metadata-count (.readInt in)]
		                                 (doall 
		                                   (for [i (range partition-metadata-count)]
		                                    {:partition-error-code (.readShort in)
		                                     :partition-id (.readInt in)
		                                     :leader (.readInt in)
		                                     :replicas  
		                                               (doall (for [i (range (.readInt in))] (.readInt in)))
		                                     :isr      (doall (for [i (range (.readInt in))] (.readInt in)))})))
		                               }
                         {:error-code error-code}))))]
           {:correlation-id correlation-id :brokers brokers :topics topics}))
                                                     
        
(defn metadata-response-decoder
  "A handler that reads metadata request responses"
  []
  (proxy [ReplayingDecoder]
    []
    (decode [ctx ^ByteBuf in ^List out] 
            ;(info "read metadata response")
            (try
             (let [resp (read-metadata-response in)]
			         (.add out resp))
             (catch Exception e (error e e)))
        )))
			        
			        
    
(defn transform [state state-transformers out]
  (let [t ((get state-transformers state) out)]
    t))

(defn checkpoint [^SafeReplayingDecoder decoder s]
  (.checkp decoder s))

(defn state [^SafeReplayingDecoder decoder]
  (.getState decoder))



 
(defn produce-response-decoder
  "A handler that reads produce responses"
  []
  (let [initial-state ProduceStates/PRODUCE_RESPONSE
        topic-len (AtomicInteger. 0)
        partition-len (AtomicInteger. 0)
        topic-name (AtomicReference. nil)
        correlation-id (AtomicReference. nil)
        end-of-consume (fn [^List out]
                         (.add out (->ResponseEnd))
                         ProduceStates/PRODUCE_RESPONSE)
        
        decrement-partition! (fn [] 
                               (if (= (.getAndDecrement partition-len) 1)
                                 (.getAndDecrement topic-len)))
        transform-messages (fn [out]
                                                       (cond 
                                                            (and (= (.get partition-len) 0)
                                                                 (= (.get topic-len) 0))
                                                            (end-of-consume out)
                                                       
                                                            (and (= (.get partition-len) 0)
                                                                 (> (.get topic-len) 0))
                                                            ProduceStates/TOPIC
                                                            (> (.get partition-len) 0)
                                                            ProduceStates/PARTITION
                                                            
                                                            :else ProduceStates/PARTITION))
        state-transformers {
                            ProduceStates/PRODUCE_RESPONSE (fn [out]
                                                             (if (> (.get topic-len) 0)
                                                                ProduceStates/TOPIC
                                                                (end-of-consume out)))
                                                             
                            ProduceStates/TOPIC            transform-messages
                            ProduceStates/PARTITION        transform-messages
                                                             
                            }
        ]
    
	  (proxy [SafeReplayingDecoder]
	    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
	    [initial-state]
	    (decode [ctx ^ByteBuf in ^List out] 
	          (let [state (state this)]
	            (cond 
	              
	              (= state ProduceStates/PRODUCE_RESPONSE)
	              (let [size (.readInt in)
                     corr-id (.readInt in)]
                 
                 (.set correlation-id corr-id)
                 (.set topic-len (.readInt in))
                 (checkpoint this (transform ProduceStates/PRODUCE_RESPONSE state-transformers out)))
               
                (= state ProduceStates/TOPIC)
	              (do
                 (.set topic-name (read-short-string in))
                 (.set partition-len (.readInt in))
                 
                 (checkpoint this (transform ProduceStates/TOPIC state-transformers out)))
               
                
	              (= state ProduceStates/PARTITION)
	              (let [partition (.readInt in)
                      error-code (.readShort in)
                      offset (.readLong in)]
                 
                 (decrement-partition!)
                 
                 (.add out
                      (->ProduceResponse (.get correlation-id) (.get topic-name) 
                                         partition error-code offset))
                 (checkpoint this (transform ProduceStates/PARTITION state-transformers out)))
               
	              :else 
	                  (throw (RuntimeException. (str "The state " state " is not excepted")))))
	             
	        ))))
			                   
          
