(ns kafka-clj.fetch
  (:require [kafka-clj.buff-utils :refer [write-short-string with-size]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_VERSION MAGIC_BYTE]])
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]))

(defn ^ByteBuf write-fecth-request-message [^ByteBuf buff {:keys [max-wait-time min-bytes topics]
                                          :or { max-wait-time 1000 min-bytes 1}}]
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]  ReplicaId => int32  MaxWaitTime => int32  MinBytes => int32  TopicName => string  Partition => int32  FetchOffset => int64  MaxBytes => int32"
  (-> buff
    (.writeInt (int -1))
    (.writeInt (int max-wait-time))
    (.writeInt (int min-bytes))
    (.writeInt (int (count topics)))) ;write topic array count
    
   (doseq [[topic partitions] topics]
	    (-> buff
       ^ByteBuf (write-short-string topic)
       (.writeInt (count partitions))) ;write partition array count
     
      (doseq [{:keys [partition offset max-bytes] :or {max-bytes (Integer/MAX_VALUE)}} partitions]
		    (-> buff
        (.writeInt (int partition))
		    (.writeLong offset)
		    (.writeInt (int max-bytes)))))
   
   buff)
	  

(defn ^ByteBuf write-fetch-request-header [^ByteBuf buff {:keys [client-id correlation-id] :or {client-id "1" correlation-id 1} :as state}]
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => FetchRequestMessage
  "
  (-> buff
     (.writeInt (int API_KEY_FETCH_REQUEST))
     (.writeInt (int API_VERSION))
     (.writeInt (int correlation-id))
     ^ByteBuf (write-short-string client-id)
     ^ByteBuf (write-fecth-request-message state)))

(defn ^ByteBuf write-fetch-request [^ByteBuf buff state]
  "Writes a fetch request api call"
    (with-size buff write-fetch-request-header state))



  
  