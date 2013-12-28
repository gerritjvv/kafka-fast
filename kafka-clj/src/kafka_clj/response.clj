(ns kafka-clj.response
  (:require [clojure.tools.logging :refer [info error]])
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

    
(defn read-short-string [^ByteBuf buff]
  (let [size (.readShort buff)
        arr  (byte-array size)]
    (.readBytes buff arr)
    (String. arr "UTF-8")))


(defn produce-response-decoder []
  "
   A handler that reads produce responses
   
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
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
      (let [msg (let [size (.readInt in)                                ;request size int
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
						               }))))]
      
			      (.add out
					        msg))
        )))
			                   
          
