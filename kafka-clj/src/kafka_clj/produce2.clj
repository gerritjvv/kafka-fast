(ns kafka-clj.produce2
  (:require [kafka-clj.codec :refer [crc32-int]])
  (:import [java.nio ByteBuffer]
           [io.netty.buffer ByteBuf Unpooled]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]))

(defn put-short-string [^ByteBuf buff s]
  (-> buff 
    (.writeShort (short (count s)))
    (.writeBytes (.getBytes (str s) "UTF-8"))))
    
(defn with-size [^ByteBuf buff f & args]
  (let [pos (.writerIndex buff)]
    (.writeInt buff (int -1))
    (apply f buff args)
    (.setInt buff (int pos) (- (.writerIndex buff) pos 4))))
      
      
(defn write-message [^ByteBuf buff ^bytes bts]
  (let [
        pos (.writerIndex buff)
        ]
    (-> buff
      (.writeInt (int -1)) ;crc32
      (.writeByte (byte 0))               ;magic
      (.writeByte (byte 0))               ;attr
      (.writeInt  (int -1))               ;nil key
      (.writeInt  (int (count bts)))      ;value bts len
      (.writeBytes bts)                   ;value bts
      
      )
    (let [arr (byte-array (- (.writerIndex buff) pos 4))]
      (.getBytes buff (+ pos 4) arr)
      (Util/setUnsignedInt buff (int pos) (crc32-int arr)))
    
    ))
      
  
(defn write-message-set [^ByteBuf buff msgs]
  (doseq [msg msgs]
    (-> buff
     
      (.writeLong 0)       ;offset
      (with-size write-message (:bts msg)) ;writes len message
      
      )))

(defn write-request [^ByteBuf buff msgs]
    (-> buff
      (.writeShort (short 0))   ;api-key
      (.writeShort (short 0))   ;version api
      (.writeInt (int 1))       ;correlation id
      (put-short-string "cid")  ;short + client-id bytes
      (.writeShort (short 1))   ;acks
      (.writeInt (int 1000)))    ;timeout
    
      (let [topic-group (group-by :topic msgs)]
        (.writeInt buff (int (count topic-group))) ;topic count
        (doseq [[topic topic-msgs] topic-group]
         (put-short-string buff topic) ;short + topic bytes
         (let [partition-group (group-by :partition topic-msgs)]
           (.writeInt buff (int (count partition-group)))  ;partition count
				   (doseq [[partition partition-msgs] partition-group]
             (.writeInt buff (int partition))       ;partition
               
				       (with-size buff write-message-set msgs))))) ;write message set with len message-set
				      
      )
  
       

(defn send-msg [topic partition msgs]
  (try 
		  (let [
		        buff (with-size (Unpooled/buffer 1024) write-request msgs)
		        
		        read-buff (ByteBuffer/allocate 1024)
		        ch (SocketChannel/open (InetSocketAddress. "localhost" (int 9092)))
		        write-int (.write ch (.nioBuffer buff))
		        read-int (try (.read ch read-buff) (catch Exception e (do (prn e) -1)))
		        ]
		    (prn "Read int " read-int)
		    (prn (java.util.Arrays/toString (-> read-buff .array)))
		    )
    (catch Exception e (.printStackTrace e))))
		  
