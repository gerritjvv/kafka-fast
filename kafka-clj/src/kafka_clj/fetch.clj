(ns kafka-clj.fetch
  (:require [clojure.tools.logging :refer [error info]]
            [clj-tuple :refer [tuple]]
            [clj-tcp.codec :refer [default-encoder]]
            [clj-tcp.client :refer [client write! read! close-all ALLOCATOR read-print-ch read-print-in]]
            [fun-utils.core :refer [fixdelay apply-get-create]]
            [kafka-clj.codec :refer [uncompress crc32-int]]
            [kafka-clj.metadata :refer [get-metadata]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.fetch-codec :refer [fetch-response-decoder bytes-read-status-handler]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_KEY_OFFSET_REQUEST API_VERSION MAGIC_BYTE]]
            [clojure.core.async :refer [go >! <! chan <!! alts!! timeout]])
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [io.netty.handler.codec ByteToMessageDecoder ReplayingDecoder]
           [io.netty.channel ChannelOption]
           [java.util List]
           [kafka_clj.util Util]
           [io.netty.channel.nio NioEventLoopGroup]))

(defn ^ByteBuf write-fecth-request-message [^ByteBuf buff {:keys [max-wait-time min-bytes topics max-bytes]
                                          :or { max-wait-time 1000 min-bytes 1 max-bytes 52428800}}]
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]  ReplicaId => int32  MaxWaitTime => int32  MinBytes => int32  TopicName => string  Partition => int32  FetchOffset => int64  MaxBytes => int32"
  ;(prn "!!!!!!!!!!!!!!!!!!!! max bytes " max-bytes)
  ;(info "min-bytes " min-bytes " max-wait-time " max-wait-time)
  (-> buff
    (.writeInt (int -1))
    (.writeInt (int max-wait-time))
    (.writeInt (int min-bytes))
    (.writeInt (int (count topics)))) ;write topic array count
   (doseq [[topic partitions] topics]
	    (-> buff
       ^ByteBuf (write-short-string topic)
       (.writeInt (count partitions))) ;write partition array count
      (doseq [{:keys [partition offset]} partitions];default max-bytes 500mb
		    (-> buff
        (.writeInt (int partition))
		    (.writeLong offset)
		    (.writeInt (int max-bytes)))))
   
   buff)
	  

(defn ^ByteBuf write-fetch-request-header [^ByteBuf buff {:keys [client-id correlation-id] :or {client-id "1" correlation-id (int (/ (System/currentTimeMillis) 1000))} :as state}]
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => FetchRequestMessage
  "
  ;(info "correlation-id " correlation-id " state " state)
  (-> buff
     (.writeShort (short API_KEY_FETCH_REQUEST))
     (.writeShort (short API_VERSION))
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

(defn read-messages [^ByteBuf buff]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [message-set-size (.readInt buff)]
    (read-messages0 buff message-set-size)))
            
        

(defn read-fetch-response [^ByteBuf in]
  "
	RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
	
  Response => CorrelationId ResponseMessage
	CorrelationId => int32
  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
  
  FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32
  "

   (let [size (.readInt in)                                ;request size int
				 correlation-id (.readInt in)                      ;correlation id int
				 topic-count (.readInt in)
         
         ]
       {:correlation-id correlation-id
        :topics 
		        (doall
		          (for [i (range topic-count)]
		            (let [topic (read-short-string in)
                      partition-count (.readInt in)]
                      [topic 
                       (doall
                         (for [p (range topic-count)]
                          {:partition (.readInt in)
                           :error-code (.readShort in)
                           :high-water-mark-offset (.readLong in)
                           :messages  (read-messages in)
                           }
                           ))])))
        }
     ))


(defn write-offset-request-message [^ByteBuf buff {:keys [topics max-offsets use-earliest] :or {use-earliest true max-offsets 10}}]
  "
	OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
	  ReplicaId => int32
	  TopicName => string
	  Partition => int32
	  Time => int64
	  MaxNumberOfOffsets => int32
	"
  (.writeInt buff (int -1)) ;replica id
  (.writeInt buff (int (count topics))) ;write topic array count
  (doseq [[topic partitions] topics]
    (write-short-string buff topic)
    (.writeInt buff (int (count partitions)))
    (doseq [{:keys [partition]} partitions]
      (do
	      (-> buff
	        (.writeInt (int partition))
	        (.writeLong (if use-earliest -2 -1))
	        (.writeInt  (int max-offsets))))))
	        
    buff)

 
(defn write-offset-request [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"} :as state}]
    (-> buff
      (.writeShort  (short API_KEY_OFFSET_REQUEST))   ;api-key
      (.writeShort  (short API_VERSION))                ;version api
      (.writeInt (int correlation-id))                  ;correlation id
      (write-short-string client-id) 
      (write-offset-request-message state)))


(defn read-offset-response [^ByteBuf in]
  "
  RequestOrResponse => Size (RequestMessage | ResponseMessage)
    Size => int32

   Response => CorrelationId ResponseMessage
    CorrelationId => int32
    ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

   OffsetResponse => [TopicName [PartitionOffsets]]
	  PartitionOffsets => Partition ErrorCode [Offset]
	  Partition => int32
	  ErrorCode => int16
	  Offset => int64
  "
  (let [size (.readInt in)                                ;request size int
				              correlation-id (.readInt in)                      ;correlation id int
				              topic-count (.readInt in)]                        ;topic array count int
				          (doall ;we must force the operation here
				              (for [i (range topic-count)]                   
						            (let [topic (read-short-string in)]                 ;topic name has len=short string bytes
						              {:topic topic
						               :partitions (let [partition-count (.readInt in)] ;read partition array count int
                                   (doall
						                             (for [q (range partition-count)]
						                               {:partition (.readInt in)        ;read partition int
						                                :error-code (.readShort in)     ;read error code short
						                                :offsets 
                                                    (let [offset-count (.readInt in)]
                                                      (doall
                                                           (for [o (range offset-count)]
                                                             (.readLong in))))}))  )     ;read offset long
						               })))))

(defn offset-response-decoder []
  "
   A handler that reads offset request responses
 
   "
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
			      (try (.add out
					        (read-offset-response in))
                (catch Exception e (do 
                                     (.printStackTrace e)
                                     (throw e)
                                     )))
        )))

(defn send-offset-request [{:keys [client conf]} topics]
  "topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]"
  (write! client (fn [^ByteBuf buff]
                    (with-size buff write-offset-request (merge conf {:topics topics}))))
  
  )

(defn send-fetch [{:keys [client conf]} topics]
  "topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]"
  
  (write! client (fn [^ByteBuf buff]
                   ;(info "!!!!!!!!!!!!>>>>>>>>>>>>>><<<<<<<<<<< write offset request; " topics)
                   ;(info "writer index " (.writerIndex buff))
                   
                   (write-fetch-request buff (merge conf {:topics topics}))
                   ;(info "after writer index " (.writerIndex buff))
                   ))
  )

(defn create-offset-producer 
  ([{:keys [host port]} conf]
    (create-offset-producer host port conf))
  ([host port conf]
    (let [c (client host port (merge  
                                   ;;parameters that can be over written
			                             {
                                   :read-group-threads 1
                                   :write-group-threads 1
                                   :reuse-client true 
                                   :read-buff 100
                                   }
                                   conf
                                   ;parameters tha cannot be overwritten
                                   {
			                             :handlers [
			                                                           offset-response-decoder
			                                                           default-encoder 
			                                                           ]}))]
      {:client c :conf conf :broker {:host host :port port}})))
                                                  
                               

(defn create-fetch-producer 
  ([{:keys [host port]} conf]
    (create-fetch-producer host port conf))
  ([host port conf]
    (let [
          ;read-group (NioEventLoopGroup. )
          ;write-group read-group
          c (client host port (merge  
                                   ;;parameters that can be over written
			                             {
                                   ;:read-group read-group
                                   ;:write-group write-group
                                   :reuse-client true 
                                   :read-buff 100
                                   ;setting default options for big data fetch
                                   ;5mb tcp receive buffer and tcp nodelay off
                                   :channel-options [
                                                     [ChannelOption/TCP_NODELAY true]
                                                     [ChannelOption/SO_RCVBUF (int 1048576)] ] 
                                   }
                                   conf
                                   ;parameters tha cannot be overwritten
                                   {
			                             :handlers [
                                                                 ;bytes-read-status-handler
			                                                           fetch-response-decoder
			                                                           #(default-encoder true) 
			                                                           ]}))]
       {:client c :conf conf :broker {:host host :port port}})))


   


 
  
  