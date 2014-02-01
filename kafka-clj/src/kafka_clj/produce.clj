(ns kafka-clj.produce
  (:require [kafka-clj.codec :refer [crc32-int compress]]
            [kafka-clj.response :refer [produce-response-decoder metadata-response-decoder]]
            [clj-tcp.client :refer [client write! read! close-all ALLOCATOR]]
            [clj-tcp.codec :refer [default-encoder]]
            [clojure.tools.logging :refer [error info]]
            [kafka-clj.buff-utils :refer [inc-capacity write-short-string with-size compression-code-mask]]
            [clj-tuple :refer [tuple]]
            [kafka-clj.msg-persist :refer [cache-sent-messages]])
  (:import [java.net InetAddress]
           [java.nio ByteBuffer]
           [java.util.concurrent.atomic AtomicLong]
           [io.netty.buffer ByteBuf Unpooled ByteBufAllocator]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]))

(defrecord Producer [client host port])
(defrecord Message [topic partition ^bytes bts])

(defonce ^:constant API_KEY_PRODUCE_REQUEST (short 0))
(defonce ^:constant API_KEY_FETCH_REQUEST (short 1))
(defonce ^:constant API_KEY_OFFSET_REQUEST (short 2))
(defonce ^:constant API_KEY_METADATA_REQUEST (short 3))


(defonce ^:constant API_VERSION (short 0))

(defonce ^:constant MAGIC_BYTE (int 0))

(defonce ^AtomicLong corr-counter (AtomicLong.))

(defn ^Long unique-corrid! []
  (let [v (.getAndIncrement corr-counter)]
    (if (= v Long/MAX_VALUE)
      (do 
        (.set corr-counter 0)
        v)
      v)))

(defn shutdown [{:keys [client]}]
  (if client
    (try 
      (close-all client)
      (catch Exception e (error e "Error while shutting down producer")))))

(defn message [topic partition ^bytes bts]
  (->Message topic partition bts))

      
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
      
  
(defn write-message-set [^ByteBuf buff correlation-id  codec msgs]
  "Writes a message set and returns a tuple of [first-offset msgs]"
      (loop [msgs1 msgs]
		    (if-let [msg (first msgs1)]
	         (do 
	            (-> buff
				      (.writeLong 0)       ;offset
				      (with-size write-message codec (:bts msg)) ;writes len message
				      )
              (recur (rest msgs1))
             )
           ))
       (tuple correlation-id msgs))
     

(defn write-compressed-message-set [^ByteBuf buff correlation-id codec msgs]
  "Writes the compressed message and returns a tuple of [offset msgs]"
  ;use the buffer allocator to get a new buffer
  (let [^ByteBufAllocator alloc (.alloc buff)
        msg-buff (.buffer alloc (int 1024))]
    (try 
		    (let [_ (write-message-set msg-buff correlation-id 0 msgs) ;write msgs to msg-buff
			        arr (byte-array (- (.writerIndex msg-buff) (.readerIndex msg-buff) ))]
			      (.readBytes msg-buff arr)
			      ;(prn "Compress out " (String. (compress codec arr)))
				    (-> buff
				      (.writeLong 0) ;offset
				      (with-size write-message codec 
				        (compress codec arr)
				        ))
             (tuple correlation-id msgs));return the (tuple offset msgs) of the uncompressed messages but with the compressed offset
       (finally ;all ByteBuff are reference counted
        (.release msg-buff)))))
		    
    
(defn write-request [^ByteBuf buff {:keys [client-id codec acks timeout] :or {client-id "1" codec 0 acks 1 timeout 1000} :as conf}
                     msgs]
  "Writes the messages and return a sequence of [ [offset msgs] ... ] The offset is the first offset in the message set
   For compressed messages this is the uncompressed message, and allows us to retry message sending."
  (let [correlation-id (unique-corrid!)]
    (-> buff
      (.writeShort  (short API_KEY_PRODUCE_REQUEST))   ;api-key
      (.writeShort  (short API_VERSION))   ;version api
      (.writeInt (int correlation-id))       ;correlation id
      (write-short-string client-id)  ;short + client-id bytes
      (.writeShort (short acks))   ;acks
      (.writeInt (int timeout)))    ;timeout
    
      (let [topic-group (group-by :topic msgs)]
        (.writeInt buff (int (count topic-group))) ;topic count
        (doall 
          (apply concat
		        (for [[topic topic-msgs] topic-group]
		         (do 
		           (write-short-string buff topic) ;short + topic bytes
			         (let [partition-group (group-by :partition topic-msgs)]
			           (.writeInt buff (int (count partition-group)))  ;partition count
		             (for [[partition partition-msgs] partition-group]
			             (do (.writeInt buff (int partition))       ;partition
				             (if (= codec 0)
				                   (with-size buff write-message-set correlation-id codec msgs)
				                   (with-size buff write-compressed-message-set  correlation-id codec msgs))))))))))))
								      
      
  
(defn read-response [{:keys [client]} timeout]
    (read! client timeout))
  
(defn- write-message-for-ack [connector conf msgs ^ByteBuf buff]
  "Writes the messages to the buff and send the results of [[offset msgs] ...] to the cache.
   This function always returns the msgs"
    (if buff (cache-sent-messages connector (with-size buff write-request conf msgs)))
    msgs)

(defn send-messages [connector
                     {:keys [client]} 
                     {:keys [acks] :as conf}
                     msgs]
  "Send messages by writing them to the tcp client.
   The write is async.
   If the conf properties acks is > 0 the messages will also be written to an inmemory cache,
   the cache expires and has a maximum size, but it allows us to retry failed messages."
  (if (> acks 0)
    (write! client (partial write-message-for-ack connector conf msgs))
	  (write! client (fn [^ByteBuf buff] 
	                       (if buff (with-size buff write-request conf msgs))
                         msgs;we must return the msgs here, its used later by the cache for retries
	                       ))))


(defn producer [host port conf]
  "returns a producer for sending messages, the decoder is a producer-response-decoder"
  (try 
  (let [c (client host port (merge  
                                   ;;parameters that can be over written
			                             {
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

(defn metadata-request-producer [host port conf]
  "Returns a producer with a metadata-response-decoder set"
  (try 
  (let [c (client host port (merge conf 
                                   {:reuse-client true :handlers [
                                                           metadata-response-decoder
                                                           default-encoder 
                                                           ]}))]
    (->Producer c host port))
  
  (catch Exception e (do
                       (error "Could not create metadata-request-producer " host " " port " due to " e)
                       (error e e)
                       (throw e)))))  
      
