(ns kafka-clj.fetch
  (:require [clojure.tools.logging :refer [error info debug]]
            [clj-tuple :refer [tuple]]
            [clj-tcp.codec :refer [default-encoder]]
            [clj-tcp.client :refer [client write! read! close-all ALLOCATOR read-print-ch read-print-in]]
            [fun-utils.core :refer [fixdelay apply-get-create]]
            [kafka-clj.codec :refer [uncompress crc32-int]]
            [kafka-clj.metadata :refer [get-metadata]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_KEY_OFFSET_REQUEST API_VERSION MAGIC_BYTE]]
            [clojure.core.async :refer [go >! <! chan >!! <!! alts!! put! timeout]])
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [io.netty.handler.codec LengthFieldBasedFrameDecoder ByteToMessageDecoder ReplayingDecoder]
           [io.netty.channel ChannelOption]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util List]
           [kafka_clj.util Util]
           [io.netty.channel.nio NioEventLoopGroup]))

(defrecord Message [topic partition offset bts])
(defrecord FetchError [topic partition error-code])

(defn ^ByteBuf write-fecth-request-message [^ByteBuf buff {:keys [max-wait-time min-bytes topics max-bytes]
                                          :or { max-wait-time 10000 min-bytes 1 max-bytes 52428800}}]
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32"
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
	  

(defonce ^AtomicInteger correlation-id-counter (AtomicInteger.))
(defn get-unique-corr-id []
  (let [v (.getAndIncrement correlation-id-counter)]
    (if (= v (Integer/MAX_VALUE)) ;check overflow
         (.set correlation-id-counter 0))
    v))


(defn ^ByteBuf write-fetch-request-header [^ByteBuf buff {:keys [client-id] :or {client-id "1"} :as state}]
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
     (.writeInt (int (get-unique-corr-id))) ;the correlation id must always be unique
     ^ByteBuf (write-short-string client-id)
     ^ByteBuf (write-fecth-request-message state)))

(defn ^ByteBuf write-fetch-request [^ByteBuf buff state]
  "Writes a fetch request api call"
    (with-size buff write-fetch-request-header state))


(declare read-message-set)
(declare read-messages0)
(declare read-messages)

(defn read-message [^ByteBuf buff topic-name partition offset state f]
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
      (let [ ^"[B" ubytes (try (uncompress codec val-arr) (catch Exception e (do  (.printStackTrace e))))
             ]
        (if ubytes
          (let [ubuff (Unpooled/wrappedBuffer ubytes)]
	                ;read the messages inside of this compressed message
                (read-messages0 ubuff (count ubytes) topic-name partition state f)
                    
             )))
      (f state (->Message topic-name partition offset val-arr))
        )))    
        
   
(defn read-message-set [^ByteBuf in reader-start bts-left topic-name partition state f]
  "MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
  (if (and (> (- bts-left 12) 0) (> (.readableBytes in) 12))
    (let [offset (.readLong in)
          message-size (.readInt in)
          bts-left2 (- bts-left 12)]
          (if (> message-size bts-left2)
	          (do 
              
             (debug "partial message found at " topic-name " " partition " bts-left " bts-left2  " message-size " message-size " readable " (.readableBytes in) " offset " offset) 
              
             (.skipBytes in (if (> bts-left2 (.readableBytes in)) (.readableBytes in) bts-left2)) state)
            
	          (read-message in topic-name partition offset state f)))
    (do 
      ;;skip any possible extra bytes
      (if (< (.readableBytes in) 12)
        (do
          (.skipBytes in (.readableBytes in))))
      
      state)))

(defn calc-bytes-read [^ByteBuf buff start-index]
    (- (.readerIndex buff) start-index))

(defn read-messages0 [^ByteBuf buff message-set-size topic-name partition state f]
  "Read all the messages till the number of bytes message-set-size have been read"
	(loop [res state reader-start (.readerIndex buff) bts-left message-set-size]
   
     (if (and (> bts-left 12) (> (.readableBytes buff) 12))
         (let [resp (read-message-set buff reader-start bts-left topic-name partition res f)]
           (recur resp
                  (.readerIndex buff) 
                  (- bts-left (calc-bytes-read buff reader-start))))
          (do
            ;we need to check for excess bytes
            (if (> bts-left 0)
              (.skipBytes buff (int bts-left)))
            res))))

(defn read-messages [^ByteBuf buff topic-name partition state f]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [message-set-size (.readInt buff)]
    (if (> message-set-size (.readableBytes buff))
      (do 
        (info "Message-set-size " message-set-size " is bigger than the readable bytes " (.readableBytes buff))
        state)
    (read-messages0 buff message-set-size topic-name partition state f))))
            

(defn read-array [^ByteBuf in state f & args]
  "Calls f with (apply f res args where res starts out as a map
   and is subsequently the result of calling the prefivous (apply f res args)"
  (let [len (.readInt in)]
    (loop [i 0 res state]
      (if (< i len)
          (recur (inc i) (apply f res args))
          res))))


(defn write-to-file [size correlation-id file ^"[B" data]
  (try (clojure.java.io/delete-file file) (catch Exception e (do )))
  (with-open [o (java.io.DataOutputStream. (java.io.FileOutputStream. (clojure.java.io/file file)))]
    (.writeInt o (int size))
    (.writeInt o (int correlation-id))
    (.write o data (int 0) (int (count data)))))

(defn read-partition [state topic-name ^ByteBuf in f]
 (let [partition (.readInt in)
       error-code (.readShort in)
       hw-mark-offset (.readLong in)]
   (if (> error-code 0)
     (let [resp (f state (->FetchError topic-name partition error-code))]
         ;;even errors have a message set size.
         (.readInt in)
         resp)
     
     (if (> (.readableBytes in) 0) 
       (read-messages in topic-name partition state f)
       state))))

(defn read-topic [state ^ByteBuf in f]
  (let [topic-name (read-short-string in)]
    (read-array in state read-partition topic-name in f)))

(defn read-fetch [^ByteBuf in state f]
  "Will return the accumelated result of f"
 (let [i (.readerIndex in)
       size (.readInt in)
       correlation-id (.readInt in)]
     (read-array in state read-topic in f)))

(comment 
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
)

(defn write-offset-request-message [^ByteBuf buff {:keys [topics max-offsets] :or {max-offsets 10}}]
  "
	OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
	  ReplicaId => int32
	  TopicName => string
	  Partition => int32
	  Time => int64
	  MaxNumberOfOffsets => int32

	  Important: use-earliest is not used here and thus all offsets are returned up to the max-offsets default 10
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
	        (.writeLong -1)
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
                   (write-fetch-request buff (merge conf {:topics topics}))
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
                                                  


(defn close-fetch-producer [{:keys [client]}]
  (if client
    (close-all client)))

(defn create-fetch-producer 
  ([{:keys [host port]} conf]
    (create-fetch-producer host port conf))
  ([host port conf]
    (if (not (and host port))
      (throw (RuntimeException. (str "A host and port must be supplied: host " host " port " port))))
    
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
                                                    #(LengthFieldBasedFrameDecoder. (Integer/MAX_VALUE) 0 4)
			                                              #(default-encoder true) 
			                                                           ]}))]
       {:client c :conf conf :broker {:host host :port port}})))


   


 
  
  