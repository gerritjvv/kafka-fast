(ns kafka-clj.fetch-tests
  (:require [kafka-clj.fetch :refer [write-fetch-request]]
            [kafka-clj.buff-utils :refer [read-short-string]])
  (:import [io.netty.buffer ByteBuf Unpooled])
  (:use midje.sweet))


(facts "Test write fetch request"
       
       (fact "Test write fetch request"
             (let [buff (Unpooled/buffer 1024)]
               (write-fetch-request buff {:topics {"a" [{:partition 1 :offset 100}]}})
               
               "RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
								ApiKey => int16
								ApiVersion => int16
								CorrelationId => int32
								ClientId => string"
               
               (.readInt buff) => 54 ;size
               (.readInt buff) => 1  ;api
               (.readInt buff) => 0  ;version
               (.readInt buff) => 1  ;default is 1 correlation id
               (read-short-string buff) => "1" ;client id
               
               "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
							  ReplicaId => int32
							  MaxWaitTime => int32
							  MinBytes => int32
							  TopicName => string
							  Partition => int32
							  FetchOffset => int64
							  MaxBytes => int32"
               (.readInt buff) => -1 ;replica id is always -1 for a client
               (.readInt buff) => 1000 ;max wait time, default is 1000
               (.readInt buff) => 1    ;min bytes default 1
               (.readInt buff) => 1    ;read topic array count
               (read-short-string buff) => "a" ;topic name
               (.readInt buff) => 1    ;partition array count
               (.readInt buff) => 1    ;partition
               (.readLong buff) => 100 ;offset
               (.readInt buff)  => (Integer/MAX_VALUE) ; max bytes
                    
               
               )
             
             ))
