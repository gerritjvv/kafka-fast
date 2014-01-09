(ns kafka-clj.fetch-codec
  (:require [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.codec :refer [uncompress crc32-int]]
            [clj-tuple :refer [tuple]])
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [io.netty.handler.codec ByteToMessageDecoder ReplayingDecoder]
           [java.util List]
           [java.util.concurrent.atomic AtomicInteger AtomicReference]
           [kafka_clj.util Util FetchStates SafeReplayingDecoder]))

"Provides a ReplayingDecoder implementation for netty,
 The decoder will read fetch responses from a broker, and return either FetchMessage [topic partition bts offset] instances 
 or a FetchError [topic partition error-code] for any messages with a error-code > 0"

(defrecord FetchMessage [topic partition ^bytes bts offset])
(defrecord FetchError [topic partition error-code])
  
(defn create-fetch-message [topic partition ^bytes bts offset]
  (->FetchMessage topic partition bts offset))

(defn create-fetch-error [topic partition error-code]
  (->FetchError topic partition error-code))

(declare read-message)

(defn read-message-set [^ByteBuf in]
  "MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
       (let [offset (.readLong in)
             message-size (.readInt in)]
		        {:offset offset
		         :message-size message-size
		         :message (read-message in)}))

(defn read-messages0 [^ByteBuf buff message-set-size]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [reader-start (.readerIndex buff)]
		  (loop [msgs []]
		    (if (< (- (.readerIndex buff) reader-start)  message-set-size)
          (recur (conj msgs (read-message-set buff)))
          msgs))))

(defn read-message [^ByteBuf buff]
  "Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
  (let [crc (Util/unsighedToNumber (.readInt buff))
        start-index (.readerIndex buff) ;start of message + attribytes + magic byte
        magic-byte (.readByte buff)
        attributes (.readByte buff)
        codec (codec-from-attributes attributes)
        key-arr (read-byte-array buff)
        val-arr (read-byte-array buff)
        end-index (.readerIndex buff)
        crc32-arr (byte-array (- end-index start-index)) 
        crc2 (do (.getBytes buff (int start-index) crc32-arr) (crc32-int crc32-arr))
        ]
    ;check attributes, if the message is compressed, uncompress and read messages
    ;else return as is
    (if (> codec 0)
      (let [ ^"[B" ubytes (try (uncompress codec val-arr) (catch Exception e (do   nil)))
             ]
        ;(prn "decompress " codec   " bts " (String. val-arr))
        (if ubytes
          (let [ubuff (Unpooled/wrappedBuffer ubytes)]
	            (doall 
			            ;read the messages inside of this compressed message
			            (mapcat flatten (map :message (read-messages0 ubuff (count ubytes))))))))
      (tuple {:crc crc :key key-arr :bts val-arr :crc2 crc2}))))
    
        
(defn checkpoint [^SafeReplayingDecoder decoder s]
  (.checkp decoder s))

(defn state [^SafeReplayingDecoder decoder]
  (.getState decoder))

(defn fetch-response-decoder []
  "
   A handler that reads fetch request responses
   "
  (let [topic-len (AtomicInteger. 0)
        partition-len (AtomicInteger. 0)
        message-set-size (AtomicInteger. 0)
        topic-name (AtomicReference. nil)
        partition-name (AtomicReference. nil)
        offset-name (AtomicReference. nil)
        initial-state (FetchStates/REQUEST_RESPONSE)
        ]
	  (proxy [SafeReplayingDecoder]
	    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
	    [initial-state]
	    (decode [ctx ^ByteBuf in ^List out] 
	      
	      (let [state (state this)]
	        (cond 
	          (= state FetchStates/REQUEST_RESPONSE)
	          (let [size (.readInt in)]
	            (checkpoint this FetchStates/RESPONSE))
	          
	          (= state FetchStates/RESPONSE)
	          (let [correlation-id (.readInt in)]
	            (checkpoint this FetchStates/FETCH_RESPONSE))
	          
	          (= state FetchStates/FETCH_RESPONSE) ;FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
	          (let [topic-count (.readInt in)]
	            (.set topic-len topic-count)
             (checkpoint this FetchStates/READ_TOPIC))
           
            (= state FetchStates/READ_TOPIC)
            (if (> (.get topic-len) 0)
	            (let [topic (read-short-string in)
	                  partition-count (.readInt in)]
	              (.set partition-len partition-count)
		            (.set topic-name topic)
                (.getAndDecrement topic-len) ;decrement topic len
	              (checkpoint this FetchStates/READ_PARTITION))
               (checkpoint this FetchStates/REQUEST_RESPONSE)) ;if all the topics have been read, reset to start
            
            (= state FetchStates/READ_PARTITION)
            (if (> (.get partition-len) 0)
              (let [partition (.readInt in)
                    error-code (.readShort in)
                    high-water-mark-offset (.readLong in)]
                (.set partition-name partition)
                (if (> error-code 0) (.add out (create-fetch-error (.get topic-name) partition error-code)))
                (.getAndDecrement partition-len) ;decrement partition length
                (checkpoint this FetchStates/READ_MESSAGES))
              (checkpoint this FetchStates/READ_TOPIC)) ;if all the partitions have been read, reset to topics
            
            (= state FetchStates/READ_MESSAGES)
            (do 
               (.set message-set-size (.readInt in))
               (checkpoint this FetchStates/READ_MESSAGE_SET))
            
            (= state FetchStates/READ_MESSAGE_SET)
            (if (> (.get message-set-size) 0)
              (let [offset (.readLong in)
                  message-size (.readInt in)]
	              (.set offset-name offset)
                (.getAndAdd message-set-size -12) ;decrement message set size by 12 bytes
	              (if (> message-size 0)
	                  (checkpoint this FetchStates/READ_MESSAGE)
	                  (checkpoint this FetchStates/READ_MESSAGE_SET))) ;back to message set
	             (checkpoint this FetchStates/READ_PARTITION)) ;if no more message-set-size go back to partition     
              
             (= state FetchStates/READ_MESSAGE)
             (let [reader-start (.readerIndex in)
                   messages (read-message in)]
               (doseq [{:keys [^"[B" bts]} messages]
                 (.add out (create-fetch-message (.get topic-name) (.get partition-name) bts (.get offset-name))))
               
               (let [bts-read (- (.readerIndex in) reader-start)]
                 (.getAndAdd  message-set-size (* -1 bts-read))
                 (checkpoint this FetchStates/READ_MESSAGE_SET)))
               
	      
	        ))))))


