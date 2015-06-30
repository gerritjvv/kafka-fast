(ns kafka-clj.response
  (:require [clojure.tools.logging :refer [info error]]
            [kafka-clj.buff-utils :refer [read-short-string]])
  (:import
    [io.netty.handler.codec ReplayingDecoder]
    [io.netty.buffer ByteBuf]
    [java.util List]
    (java.io DataInputStream)))

(defrecord ResponseEnd [])
(defrecord ProduceResponse [correlation-id topic partition error-code offset])

(defn error? [error-code]
  (> error-code 0))

(comment
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
                                     }))

(defn read-metadata-response [^ByteBuf in]
  (let [_ (.readInt in)                        ;request size
        correlation-id (.readInt in)           ;correlation id
        broker-count (.readInt in)             ;broker array len
        brokers   (doall 
	                  (for [_ (range broker-count)]
	                    {:node-id (.readInt in)
	                     :host (read-short-string in)
	                     :port (.readInt in)}))
        topic-metadata-count (.readInt in)
        topics (doall
                 (for [_ (range topic-metadata-count)]
                   (let [error-code (.readShort in) 
                         topic (read-short-string in)]
	                   (if topic
		                   {:error-code error-code
		                    :topic topic
		                    :partitions
		                                (let [partition-metadata-count (.readInt in)]
		                                 (doall 
		                                   (for [_ (range partition-metadata-count)]
		                                    {:partition-error-code (.readShort in)
		                                     :partition-id (.readInt in)
		                                     :leader (.readInt in)
		                                     :replicas  
		                                               (doall (for [_ (range (.readInt in))] (.readInt in)))
		                                     :isr      (doall (for [_ (range (.readInt in))] (.readInt in)))})))
		                               }
                         {:error-code error-code}))))]
           {:correlation-id correlation-id :brokers brokers :topics topics}))
                                                     
        
(defn metadata-response-decoder
  "A handler that reads metadata request responses"
  []
  (proxy [ReplayingDecoder]
    []
    (decode [_ ^ByteBuf in ^List out]
            ;(info "read metadata response")
            (try
             (let [resp (read-metadata-response in)]
			         (.add out resp))
             (catch Exception e (error e e))))))
			        
			        


(defn read-int [^DataInputStream in]
  (.readInt in))

(defn read-long [^DataInputStream in]
  (.readLong in))

(defn read-short2 [^DataInputStream in]
  (.readShort in))

(defn read-short-string2 [^DataInputStream in]
  (let [size (read-short2 in)]
    (if (pos? size)
      (let [arr  (byte-array size)]
        (.read in arr)
        (String. arr "UTF-8")))))

(defn lazy-array-read [len f] (repeatedly len f))

(defn inpartition->kafkarespseq [in corrid topic-name]
  (->ProduceResponse corrid
                     topic-name
                     (long (read-int in))                          ;partition
                     (long (read-short2 in))                       ;error-code
                     (long (read-long in))))                       ;offset

(defn intopic->kafkarespseq [in corrid]
  (let [topic-name (read-short-string2 in)
        partition-len (read-int in)]
    (lazy-array-read partition-len #(inpartition->kafkarespseq in corrid topic-name))))

(defn in->kafkarespseq [^DataInputStream in]
  (let [corrid (read-int in)
        topic-len (read-int in)]
    (lazy-array-read topic-len #(intopic->kafkarespseq in corrid))))

