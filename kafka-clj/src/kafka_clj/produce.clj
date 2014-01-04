(ns kafka-clj.produce
  (:require [kafka-clj.codec :refer [crc32-int get-compress-out compress]]
            [kafka-clj.response :refer [produce-response-decoder metadata-response-decoder]]
            [clj-tcp.client :refer [client write! read! close-all ALLOCATOR]]
            [clj-tcp.codec :refer [default-encoder]]
            [kafka-clj.buff-utils :refer [write-short-string with-size compression-code-mask]])
  (:import [java.net InetAddress]
           [java.nio ByteBuffer]
           [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]))

(defrecord Producer [client host port])
(defrecord Message [topic partition ^bytes bts])

(defonce ^:constant API_KEY_PRODUCE_REQUEST (short 0))
(defonce ^:constant API_KEY_FETCH_REQUEST (short 1))
(defonce ^:constant API_KEY_METADATA_REQUEST (short 3))

(defonce ^:constant API_VERSION (short 0))

(defonce ^:constant MAGIC_BYTE (int 0))

(defn shutdown [{:keys [client]}]
  (if client
    (close-all client)))

(defn message [topic partition ^bytes bts]
  (->Message topic partition bts))

(defn ^ByteBuf inc-capacity [^ByteBuf bytebuf l]
  (let [len (+ (.capacity bytebuf) (int l))]
    (if (> len (.maxCapacity bytebuf))
      (.capacity bytebuf len))
    bytebuf))

      
(defn write-message [^ByteBuf buff codec ^bytes bts]
  (let [
        pos (.writerIndex buff)
        ]
    (-> buff
      (.writeInt (int -1)) ;crc32
      (.writeByte (byte 0))               ;magic
      (.writeByte (int (bit-or (byte 0) (bit-and compression-code-mask codec))));attr
      (.writeInt  (int -1))               ;nil key
      (.writeInt  (int (count bts)))      ;value bts len
      (.writeBytes bts)                   ;value bts
      
      )
    (let [arr (byte-array (- (.writerIndex buff) pos 4))]
      (.getBytes buff (+ pos 4) arr)
      (Util/setUnsignedInt buff (int pos) (crc32-int arr)))
    
    ))
      
  
(defn write-message-set [^ByteBuf buff codec msgs]
	  (doseq [msg msgs]
      (prn "Writing message msg " (String. (:bts msg)))
	    (-> buff
	     
	      (.writeLong 0)       ;offset
	      (with-size write-message codec (:bts msg)) ;writes len message
	      
	      )))
     

(defn write-compressed-message-set [^ByteBuf buff codec msgs]
  (let [msg-buff (Unpooled/buffer 1024)]
    
    (write-message-set msg-buff 0 msgs) ;write msgs to msg-buff
    (let [arr (byte-array (- (.writerIndex msg-buff) (.readerIndex msg-buff) ))]
      (.readBytes msg-buff arr)
      
	    (-> buff
	      (.writeLong 0) ;offset
	      (with-size write-message codec 
	        (compress codec arr)
	        )))))
	    
    
(defn write-request [^ByteBuf buff {:keys [correlation-id client-id codec acks timeout] :or {correlation-id 1 client-id "1" codec 0 acks 1 timeout 1000}}
                     msgs]
    (-> buff
      (.writeShort  (short API_KEY_PRODUCE_REQUEST))   ;api-key
      (.writeShort  (short API_VERSION))   ;version api
      (.writeInt (int correlation-id))       ;correlation id
      (write-short-string client-id)  ;short + client-id bytes
      (.writeShort (short acks))   ;acks
      (.writeInt (int timeout)))    ;timeout
    
      (let [topic-group (group-by :topic msgs)]
        (.writeInt buff (int (count topic-group))) ;topic count
        (doseq [[topic topic-msgs] topic-group]
         (write-short-string buff topic) ;short + topic bytes
         (let [partition-group (group-by :partition topic-msgs)]
           (.writeInt buff (int (count partition-group)))  ;partition count
				   (doseq [[partition partition-msgs] partition-group]
             (.writeInt buff (int partition))       ;partition
               
				       (if (= codec 0)
                   (with-size buff write-message-set codec msgs)
                   (with-size buff write-compressed-message-set codec msgs)))))) ;write message set with len message-set
				      
      )
  
(defn read-response [{:keys [client]} timeout]
    (read! client timeout))
       
(defn send-messages [{:keys [client]} 
                     conf
                     msgs]
  (write! client (fn [^ByteBuf buff] 
                   (with-size buff write-request conf msgs)
                   )))


(defn producer [host port conf]
  "returns a producer for sending messages, the decoder is a producer-response-decoder"
  (try 
    (prn "NEW PRODUCER 1 !!!!!!")
  (let [c (client host port (merge  
                                   ;;parameters that can be over written
			                             {:max-concurrent-writes 4000
			                             :reuse-client true 
                                   :write-buff 100
                                   }
                                   ;merge conf
                                   conf
                                   ;parameters tha cannot be overwritten
                                   {
			                             ;:channel-options [[ALLOCATOR (PooledByteBufAllocator. false)]]
			                             :handlers [
			                                                           produce-response-decoder
			                                                           default-encoder 
			                                                           
			                                                           ]}))]
    (->Producer c host port))
  (catch Exception e (.printStackTrace e))))
  


;; ------- METADATA REQUEST API

(defn write-metadata-request [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"}}]
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    ApiKey => int16
    ApiVersion => int16
    CorrelationId => int32
    ClientId => string
    MetadataRequest => [TopicName]
       TopicName => string
   "
    (-> buff
      (.writeShort  (short API_KEY_METADATA_REQUEST))   ;api-key
      (.writeShort  (short API_VERSION))                ;version api
      (.writeInt (int correlation-id))                  ;correlation id
      (write-short-string client-id)                      ;short + client-id bytes
      (.writeInt (int 0))))                            ;write empty topic, dont use -1 (this means nil), list to receive metadata on all topics
      
      
(defn send-metadata-request [{:keys [client]} conf]
  "Writes out a metadata request to the producer's client"
      (write! client (fn [^ByteBuf buff] 
                       (with-size buff write-metadata-request conf)
                   )))

(defn metadata-request-producer [host port]
  "Returns a producer with a metadata-response-decoder set"
  (prn "try metadata producer " host ":" port)
  (try 
  (let [c (client host port {:reuse-client true :handlers [
                                                           metadata-response-decoder
                                                           default-encoder 
                                                           ]})]
    (->Producer c host port))
  
  (catch Exception e (do 
                       (.printStackTrace e)
                       (throw e)))))  
      
