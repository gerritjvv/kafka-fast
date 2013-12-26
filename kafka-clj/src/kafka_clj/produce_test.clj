(ns kafka-clj.produce-test
  (:require [kafka-clj.codec :refer [crc32-int]])
  (:import [java.nio ByteBuffer]
           [io.netty.buffer ByteBuf Unpooled]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]))

(defn put-short-string [^ByteBuf buff s]
  (-> buff 
    (.writeShort (short (count s)))
    (.writeBytes (.getBytes (str s) "UTF-8"))))
    
(defn with-size [^ByteBuf buff f & args]
  (let [pos (.writerIndex buff)]
    (.writeInt buff (int -1))
    (apply f buff args)
    (.setInt buff (int pos) (- (.writerIndex buff) pos 4))))
      
      
(defn write-message [^ByteBuf buff]
  (let [bts (.getBytes "HI" "UTF-8")
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
      (.setInt buff (int pos) (int (crc32-int arr))))
    
    ))
      
  
(defn write-message-set [^ByteBuf buff]
    (-> buff
     
      (.writeLong 0)       ;offset
      (with-size write-message) ;writes len message
      
      ))

(defn write-request [^ByteBuf buff]
    (-> buff
      (.writeShort (short 0))   ;api-key
      (.writeShort (short 0))   ;version api
      (.writeInt (int 1))       ;correlation id
      (put-short-string "cid")  ;short + client-id bytes
      (.writeShort (short 1))   ;acks
      (.writeInt (int 1000))    ;timeout
      (.writeInt (int 1))       ;topic count
      (put-short-string "ping") ;short + topic bytes
      (.writeInt (int 1))       ;partition count
      (.writeInt (int 0))       ;partition
      
      (with-size write-message-set) ;write message set with len message-set
      
      ))
  
       

(defn send-msg [topic partition]
  (try 
		  (let [
		        buff (with-size (Unpooled/buffer 1024) write-request)
		        
		        read-buff (ByteBuffer/allocate 1024)
		        ch (SocketChannel/open (InetSocketAddress. "localhost" (int 9092)))
		        write-int (.write ch (.nioBuffer buff))
		        read-int (try (.read ch read-buff) (catch Exception e (do (prn e) -1)))
		        ]
		    (prn "Read int " read-int)
		    (prn (java.util.Arrays/toString (-> read-buff .array)))
		    )
    (catch Exception e (.printStackTrace e))))
		  
