(ns kafka-clj.producer
  (:require [kafka-clj.codec :refer [get-compress-out get-codec-int crc32-int]]
            [clojure.core.reducers :as r]
            [clj-tcp.client :refer [client write!]]
            [fmap-clojure.core :refer [>>=*]])
   (:import [java.io DataOutputStream ByteArrayOutputStream]
           [io.netty.buffer ByteBuf Unpooled]
           [kafka_clj.util Util]))

;(defrecord Client [group channel-f write-ch read-ch error-ch ^AtomicInteger reconnect-count ^AtomicBoolean closed])
(defrecord Producer [client host port])

(defonce ^:constant MAGIC_BYTE (int 0))
(defonce ^:constant compression-code-mask 0x03)

(defn ^ByteBuf inc-capacity [^ByteBuf bytebuf l]
  (let [len (+ (.capacity bytebuf) (int l))]
    (if (> len (.maxCapacity bytebuf))
      (.capacity bytebuf len))
    bytebuf))

(defn ^ByteBuf write-message [^ByteBuf buff codec-int ^bytes bts]
  "Write the message format
   crc32 int
   magic byte
   attributes byte"
  (-> ^ByteBuf buff 
    (Util/writeUnsignedInt (crc32-int bts))
    (.writeByte (int MAGIC_BYTE))
    (.writeByte (int (bit-or (byte 0) (bit-and compression-code-mask codec-int))))
    (.writeInt (int -1))
    (.writeBytes bts)))

(defn ^ByteBuf write-message-set [^ByteBuf buff codec-int bts]
  "A message set is simply a message with headers"
  (inc-capacity buff (+ 22 (count bts)))
  (.writeLong buff 0) ;the offset is not used during produce requests
  (let [len-writer-index (.writerIndex buff)]
    (.writeInt buff (int -1))
	  (if bts
	     (do 
	       (write-message buff codec-int bts)
	       (let [len (- (.readableBytes buff) len-writer-index 4)]
		       (.setInt buff (int len-writer-index) (int len))
	         )))
	    buff))
		     
(defn pack-messages [^ByteBuf buff codec-int msgs]
  "Pack a list of messages 
   if the codec-int is 1 or 2, then the messages is compressed
   and the compressed bytes written again as a message set"
  (let [^ByteBuf msgs-buff (r/fold (fn ([] (Unpooled/buffer 1024))
                                ([^ByteBuf buff1 msg]
                                   (write-message-set buff1 0 (:bts msg)))) msgs)]
    (if (= codec-int 0)
      (.writeBytes buff msgs-buff)
      (write-message-set buff codec-int (.array msgs-buff)))
    
    buff))
 
(defn ^ByteBuf write-produce-request [^ByteBuf buff codec partition msgs {:keys [acks timeout] :or {acks 2 timeout 500}}]
  "Write out the produce request to the buff"
	  (doto buff
	    (.writeShort (short acks))
	    (.writeInt (int timeout))
	    (.writeInt (int partition)))
	  
    (let [codec-int (get-codec-int codec)
          len-writer-index (.writerIndex buff)]
	    (.writeInt buff (int -1))
	    (pack-messages buff codec-int msgs)
	    (let [len (- (.readableBytes buff) len-writer-index 4)]
       (.setInt buff (int len-writer-index) (int len))))
    buff)
    
  
(defn send-message [{:keys [client]} {:keys [bts]}]
  (write! client bts))
  
  
(defn producer [host port]
  (let [c (client host port {:reuse-client true})]
    (->Producer c host port)))
  
    
  

