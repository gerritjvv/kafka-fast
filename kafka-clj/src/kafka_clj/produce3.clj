(ns kafka-clj.produce3
  (:require [kafka-clj.codec :refer [crc32-int get-compress-out compress]]
            [clj-tcp.client :refer [client write!]])
  (:import [java.net InetAddress]
           [java.nio ByteBuffer]
           [io.netty.buffer ByteBuf Unpooled]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]))

(defrecord Producer [client host port])

(defonce ^:constant API_KEY_PRODUCE_REQUEST (short 0))
(defonce ^:constant API_VERSION (short 0))

(defonce ^:constant MAGIC_BYTE (int 0))
(defonce ^:constant compression-code-mask 0x03)

(defn ^ByteBuf inc-capacity [^ByteBuf bytebuf l]
  (let [len (+ (.capacity bytebuf) (int l))]
    (if (> len (.maxCapacity bytebuf))
      (.capacity bytebuf len))
    bytebuf))


(defn put-short-string [^ByteBuf buff s]
  (-> buff 
    (.writeShort (short (count s)))
    (.writeBytes (.getBytes (str s) "UTF-8"))))
    
(defn with-size [^ByteBuf buff f & args]
  (let [pos (.writerIndex buff)]
    (.writeInt buff (int -1))
    (apply f buff args)
    (.setInt buff (int pos) (- (.writerIndex buff) pos 4))))
      
      
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
      (put-short-string client-id)  ;short + client-id bytes
      (.writeShort (short acks))   ;acks
      (.writeInt (int timeout)))    ;timeout
    
      (let [topic-group (group-by :topic msgs)]
        (.writeInt buff (int (count topic-group))) ;topic count
        (doseq [[topic topic-msgs] topic-group]
         (put-short-string buff topic) ;short + topic bytes
         (let [partition-group (group-by :partition topic-msgs)]
           (.writeInt buff (int (count partition-group)))  ;partition count
				   (doseq [[partition partition-msgs] partition-group]
             (.writeInt buff (int partition))       ;partition
               
				       (if (= codec 0)
                   (with-size buff write-message-set codec msgs)
                   (with-size buff write-compressed-message-set codec msgs)))))) ;write message set with len message-set
				      
      )
  
       
(defn send-messages [{:keys [client]} 
                     conf
                     msgs]
  (write! client (fn [^ByteBuf buff] 
                   (with-size buff write-request conf msgs)
                   )))

  
(defn producer [host port]
  (try 
  (let [c (client host port {:reuse-client true})]
    (->Producer c host port))
  (catch Exception e (.printStackTrace e))))
  
