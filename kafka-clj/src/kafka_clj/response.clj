(ns kafka-clj.response
  (:require [clojure.tools.logging :refer [info error]]
            [kafka-clj.buff-utils :refer [read-short-string]])
  (:import 
           [io.netty.handler.codec ByteToMessageDecoder ReplayingDecoder]
           [io.netty.buffer ByteBuf]
           [java.util List]
           ))

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

    

(defn read-produce-response [^ByteBuf in]
  "
  RequestOrResponse => Size (RequestMessage | ResponseMessage)
    Size => int32

   Response => CorrelationId ResponseMessage
    CorrelationId => int32
    ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

   ProduceResponse => [TopicName [Partition ErrorCode Offset]]
	  TopicName => string
	  Partition => int32
	  ErrorCode => int16
	  Offset => int64
  "
  (let [size (.readInt in)                                ;request size int
				              correlation-id (.readInt in)                      ;correlation id int
				              topic-count (.readInt in)]                        ;topic array count int
				          (doall ;we must force the operation here
				              (for [i (range topic-count)]                   
						            (let [topic (read-short-string in)]                 ;topic name has len=short string bytes
						              {:topic topic
						               :partitions (let [partition-count (.readInt in)] ;read partition array count int
						                             (for [q (range partition-count)]
						                               {:partition (.readInt in)        ;read partition int
						                                :error-code (.readShort in)     ;read error code short
						                                :offset (.readLong in)}))       ;read offset long
						               })))))

(defn read-metadata-response [^ByteBuf in]
  "
		RequestOrResponse => Size (RequestMessage | ResponseMessage)
		    Size => int32

	   Response => CorrelationId ResponseMessage
	    CorrelationId => int32

		MetadataResponse => [Broker][TopicMetadata]
		  Broker => NodeId Host Port
		  NodeId => int32
		  Host => string
		  Port => int32
		  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
		  TopicErrorCode => int16
		  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
		  PartitionErrorCode => int16
		  PartitionId => int32
		  Leader => int32
		  Replicas => [int32]
		  Isr => [int32]
		"
  (let [size (.readInt in)                     ;request size
        correlation-id (.readInt in)           ;correlation id
        broker-count (.readInt in)             ;broker array len
        brokers (doall 
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
                                                     
        
(defn metadata-response-decoder []
  "
   A handler that reads metadata request responses
 
   "
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
			      (.add out
					        (read-metadata-response in))
        )))
			        

(defn produce-response-decoder []
  "
   A handler that reads produce responses
 
   "
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
			      (.add out
					        (read-produce-response in))
        )))
			                   
          
