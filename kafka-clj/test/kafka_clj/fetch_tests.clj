(ns kafka-clj.fetch-tests
  (:require [kafka-clj.fetch :refer [write-fetch-request read-messages]]
            [kafka-clj.produce :as produce]
            [kafka-clj.buff-utils :refer [read-short-string]])
  (:import [io.netty.buffer ByteBuf Unpooled])
  (:use midje.sweet))


(facts "Test write fetch request"
       
      (fact "Test write fetch request"
             (let [buff (Unpooled/buffer 1024)]
               (write-fetch-request buff {:topics [["raw-requests-adx" [{:offset 0, :error-code 0, :locked true, :partition 7} 
                                                                        {:offset 0, :error-code 0, :locked true, :partition 5} 
                                                                        {:offset 0, :error-code 0, :locked true, :partition 3}]]]})
               
               "RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
                  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest
   FetchRequest

FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32
   "
                
                (prn ">>>>>>> len: " (.readInt buff))
                (prn ">>>>>>> apikey: " (.readShort buff))
                (prn ">>>>>>> api-version: " (.readShort buff))
                (prn ">>>>>>> corrid: " (.readInt buff))
                (prn ">>>>>>> cid: " (read-short-string buff))
                 
                (.readInt buff) => -1
                (prn ">>> max wait time " (.readInt buff))
                (prn ">>> min bytes " (.readInt buff))
                (prn "topics " (.readInt buff))
                (prn "topic " (read-short-string buff))
                (let [partition-len (.readInt buff)]
                  partition-len => 3
                  (.readInt buff) => 7
                  (.readLong buff) => 0
                  (.readInt  buff)
                  
                  (.readInt buff) => 5
                  (.readLong buff) => 0
                  (.readInt  buff)
                  
                  (.readInt buff) => 3
                  (.readLong buff) => 0
                  (.readInt  buff)
                  
                  )
                (prn "Reabable bytes " (.readableBytes buff))
                
                
              
               )
             
             )
       
       )
