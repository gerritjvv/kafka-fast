(ns kafka-clj.fetch
  (:require [clj-tuple :refer [tuple]]
            [kafka-clj.codec :refer [uncompress]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
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


(declare read-message-set)
(declare read-messages0)

(defn read-message [^ByteBuf buff]
  "Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
  (let [crc (.readInt buff)
        magic-byte (.readByte buff)
        attributes (.readByte buff)
        codec (codec-from-attributes attributes)
        key-arr (read-byte-array buff)
        val-arr (read-byte-array buff)]
    ;check attributes, if the message is compressed, uncompress and read messages
    ;else return as is
    
    (if (> codec 0)
      (let [ ^"[B" ubytes (uncompress codec val-arr)
             ubuff (Unpooled/wrappedBuffer ubytes)
             ]
          (doall 
            ;read the messages inside of this compressed message
            (mapcat flatten (map :message (read-messages0 ubuff (count ubytes))))))
      (tuple {:crc crc :key key-arr :bts val-arr}))))
    
        
   
(defn read-message-set [^ByteBuf buff]
  "MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
  
        {:offset (.readLong buff)
         :message-size (.readInt buff)
         :message (read-message buff)})

(defn read-messages0 [^ByteBuf buff message-set-size]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [reader-start (.readerIndex buff)]
		  (loop [msgs []]
        (prn  " " (.readerIndex buff) " diff < messsage-set-size " (< (- (.readerIndex buff) reader-start)  message-set-size))
		    (if (< (- (.readerIndex buff) reader-start)  message-set-size)
          (recur (conj msgs (read-message-set buff)))
          msgs))))

(defn read-messages [^ByteBuf buff]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [message-set-size (.readInt buff)]
    (read-messages0 buff message-set-size)))
            
        

(defn read-fecth-response [^ByteBuf buff]
  "
	Response => CorrelationId ResponseMessage
	CorrelationId => int32
	ResponseMessage

  FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32
  MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"

  (let [size (.readInt buff)
        correlation-id (.readInt buff)
        topic-count (.readInt buff)]
    (doall
	      (for [i (range topic-count)
	          :let [topic-name (read-short-string buff)
	                partition-count (.readInt buff)]]
	          
	          (for [q (range (partition-count))]
	            {:partition (.readInt buff)
	             :error-codec (.readShort buff)
	             :high-water-mark-offset (.readLong buff)
	             :messages (read-messages buff)})))))

  
  