(ns
  ^{:author "gerritjvv"
    :doc "Raw api for sending and reading kafka fetch offset and fetch messages requests,
          reading the fetch message is done by the java class  kafka_clj.util.Fetch"}
  kafka-clj.fetch
  (:require [clojure.tools.logging :refer [error info debug]]
            [clj-tuple :refer [tuple]]
            [clj-tcp.codec :refer [default-encoder]]
            [kafka-clj.tcp :as tcp]
            [clj-tcp.client :refer [write! client close-all]]
            [fun-utils.core :refer [fixdelay apply-get-create]]
            [kafka-clj.codec :refer [uncompress crc32-int]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_KEY_OFFSET_REQUEST API_VERSION MAGIC_BYTE]]
            [clojure.core.async :refer [go >! <! chan >!! <!! alts!! put! timeout]])
  (:import [io.netty.buffer ByteBuf]
           [io.netty.handler.codec ReplayingDecoder]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util List]))


(defn ^ByteBuf write-fecth-request-message [^ByteBuf buff {:keys [max-wait-time min-bytes topics max-bytes]
                                                           :or { max-wait-time 1000 min-bytes 100 max-bytes 104857600}}]
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32"
  ;(prn "!!!!!!!!!!!!!!!!!!!! max bytes " max-bytes)
  ;(info "min-bytes " min-bytes " max-bytes " max-bytes " max-wait-time " max-wait-time)
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
  (let [_ (.readInt in)                                ;request size int
        _ (.readInt in)                      ;correlation id int
        topic-count (.readInt in)]                        ;topic array count int
    (doall ;we must force the operation here
      (for [_ (range topic-count)]
        (let [topic (read-short-string in)]                 ;topic name has len=short string bytes
          {:topic topic
           :partitions (let [partition-count (.readInt in)] ;read partition array count int
                         (doall
                           (for [_ (range partition-count)]
                             {:partition (.readInt in)        ;read partition int
                              :error-code (.readShort in)     ;read error code short
                              :offsets
                              (let [offset-count (.readInt in)]
                                (doall
                                  (for [_ (range offset-count)]
                                    (.readLong in))))}))  )     ;read offset long
           })))))

(defn offset-response-decoder []
  "
   A handler that reads offset request responses

   "
  (proxy [ReplayingDecoder]
         ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
         []
    (decode [_ ^ByteBuf in ^List out]
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

(defn create-offset-producer
  ([{:keys [host port]} conf]
   (create-offset-producer host port conf))
  ([host port conf]
   (let [c (client host port (merge
                               ;;parameters that can be over written
                               {
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

(defn send-fetch [{:keys [client conf]} topics]
  {:pre [client (coll? topics)]}
  "Version2: send-fetch requires a kafka-clj.tcp client
   topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]

   Sends fetch request"
  (tcp/write! client
              (write-fetch-request (tcp/empty-byte-buff) (merge conf {:topics topics}))
              :flush true))



 
  
  