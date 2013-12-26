(ns kafka-clj.producer
  (:require [clojure.tools.logging :refer [info error]]
            [kafka-clj.codec :refer [get-compress-out get-codec-int crc32-int]]
            [clojure.core.reducers :as r]
            [clj-tcp.client :refer [client write!]]
            [fmap-clojure.core :refer [>>=*]]
            [clojure.pprint :refer [pprint]])
   (:import [java.io DataOutputStream ByteArrayOutputStream]
           [io.netty.buffer ByteBuf Unpooled]
           [kafka_clj.util Util]))

"
Kafka protocol format:
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TheProtocol

"

(declare read-produce-request)

;(defrecord Client [group channel-f write-ch read-ch error-ch ^AtomicInteger reconnect-count ^AtomicBoolean closed])
(defrecord Producer [client host port])

(defonce ^:constant API_KEY_PRODUCE_REQUEST (int 0))
(defonce ^:constant API_VERSION (int 0))

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
    (.writeInt (int (count bts)))
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
 
(defn ^ByteBuf write-produce-request [^ByteBuf buff codec partition {:keys [acks timeout] :or {acks 2 timeout 500}} msgs]
  "Write out the produce request to the buff"
	  (doto buff
	    (.writeShort (short acks))
	    (.writeInt (int timeout)))
     
   (let [group-by-topic (group-by :topic msgs)]
     (.writeInt buff (int (if group-by-topic (count group-by-topic) -1)) ) ;the number of topic messages
	   (doseq [[topic topic-msgs] group-by-topic]
	       ;write the topic
         (pprint (str "!!!!!!!!!!!!!! topic count pos " (.writerIndex buff) "  topic count " (count topic)))
		     (.writeShort buff (short (count topic)))
		     (.writeBytes buff (.getBytes (str topic) "UTF-8"))
       
		     (let [group-by-partition (group-by :partition topic-msgs)]
             (pprint (str "!!!!!!!!!!!!!! partition count pos " (.writerIndex buff) "  partition count " (count group-by-partition)))
             (.writeInt buff (int (if group-by-partition (count group-by-partition) -1)) ) ;the number of partition messages
				     ;for each partition in the topic write the messages
				     (doseq [[partition t-p-msgs] group-by-partition]
                  (pprint (str "!!!!!!!!!!!!!!!!!!!!!! partition index " (.writerIndex buff)  " partition " (int partition)))
						      (.writeInt buff (int partition))
							    
						      ;write all the messages in this partition
							    (let [codec-int (get-codec-int codec)
							          len-writer-index (.writerIndex buff)]
								    (.writeInt buff (int -1))
								    (pack-messages buff codec-int t-p-msgs)
								    (let [len (- (.readableBytes buff) 4)]
							       (.setInt buff (int len-writer-index) (int len)))))))
	       buff))
    

(defn ^ByteBuf pack-request [^ByteBuf buff correlation-id client-id codec partition request-msg-f conf msgs]
  "Wites the RequestMessage header information and delegates actual message writing to the requst-msg-f"
  (.writeShort buff (short API_KEY_PRODUCE_REQUEST))
  (.writeShort buff (short API_VERSION))
  (.writeInt buff (int correlation-id))
  (.writeShort buff (short (count client-id)))
  (.writeBytes buff ^bytes (.getBytes (str client-id) "UTF-8"))
  (request-msg-f buff codec partition conf msgs))
  
(defn ^ByteBuf write-request [^ByteBuf buff correlation-id client-id codec partition request-msg-f conf msgs]
  (inc-capacity buff 1024)
  (let [pos (.writerIndex buff)]
    (.writeInt buff -1)
    (pack-request buff correlation-id client-id codec partition request-msg-f conf msgs)
    (.setInt buff (int pos) (int (- (.readableBytes buff) 4))))
  buff)
  
(defn send-messages [{:keys [client]} correlation-id client-id codec partition conf msgs]
  (write! client (fn [^ByteBuf buff] 
                   ;called from inside the Netty DefaultCodec MessageToByteEncoder
                   (info "!!!!!!!!!!!!!!!!!!!!!! Inside encoder")
                   (write-request buff correlation-id client-id codec partition write-produce-request conf msgs) 
                   (pprint (read-produce-request buff))
               
                   )))
  
  
(defn producer [host port {:keys [acts timeout] }]
  (try 
  (let [c (client host port {:reuse-client true})]
    (->Producer c host port))
  (catch Exception e (.printStackTrace e))))
  
  
(defn read-bytes-value [^ByteBuf buff]
  (let [len (.readInt buff)
        arr (byte-array len)]
    (.readBytes buff arr)
    arr))

(defn read-string-value [^ByteBuf buff]
  (let [len (.readShort buff)
        arr (byte-array len)]
    (.readBytes buff arr)
    (String. arr "UTF-8")))

;;used for testing
(defn read-produce-request [^ByteBuf buff]
  (let [request-size (.readInt buff)
        api-key (.readShort buff)
        api-version (.readShort buff)
        correlation-id (.readInt buff)
        client-id (read-string-value buff)
        required-acks (.readShort buff)
        timeout (.readInt buff)
        topic-count (.readInt buff)
        
        topics (for [i (range topic-count)]
                 {
                   :topic-pos (.readerIndex buff)
                   :topic (read-string-value buff)
                   :partitions (let [
                                     partition-count (.readInt buff)
                                     partitions (doall (for [i (range 1)] (try 
                                                                            (do 
                                                                              
                                                                              (let [partition (.readInt buff)
                                                                                    message-set-size (.readInt buff)
                                                                                    off-set (.readLong buff)
                                                                                    message-size (.readInt buff)
                                                                                    arr (byte-array message-size)
                                                                                    ]
                                                                                (.readBytes buff arr)
                                                                                {:partition partition
                                                                                 :message-set-size message-set-size
                                                                                 :off-set off-set
                                                                                 :message-size message-size
                                                                                 :message (String. arr)}
                                                                              
                                                                              )) (catch Exception e (do nil))) ) )
                                                 
		                                                 ]
                   {
                    :partition-count partition-count
                    :partitions partitions
                    })
                                       
                                   
                   })
        ]
    
    
    {:request-size request-size
     :api-key api-key
     :api-version api-version
     :correlation-id correlation-id
     :client-id {:len (count client-id) :val client-id} 
     :produce-request {:required-acks required-acks :timeout timeout
                       :topic-count topic-count
                       
                       :topics topics
                       }
     }))



  
  

  

