(ns
  ^{:author "gerritjvv"
    :doc    "Raw api for sending and reading kafka fetch offset and fetch messages requests,
          reading the fetch message is done by the java class  kafka_clj.util.Fetch"}
  kafka-clj.fetch
  (:require [clojure.tools.logging :refer [error info debug]]
            [clj-tuple :refer [tuple]]
            [kafka-clj.tcp :as tcp]
            [fun-utils.core :refer [fixdelay apply-get-create]]
            [kafka-clj.codec :refer [uncompress crc32-int]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_KEY_OFFSET_REQUEST API_VERSION MAGIC_BYTE]]
            [clojure.core.async :refer [go >! <! chan >!! <!! alts!! put! timeout]]
            [tcp-driver.io.stream :as tcp-stream]
            [schema.core :as s]
            [tcp-driver.driver :as tcp-driver]
            [kafka-clj.schemas :as schemas])
  (:import [io.netty.buffer ByteBuf Unpooled]
           [java.util.concurrent.atomic AtomicInteger]
           (kafka_clj.util Util)))


(defn ^ByteBuf write-fecth-request-message
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32

  topics = [[\"<topic-name>\" [{:partition <partition>, :offset <offset-to-fetch-from>}]]]
  "
  [^ByteBuf buff
   topics
   {:keys [max-wait-time min-bytes max-bytes]
    :or   {max-wait-time 1000 min-bytes 100 max-bytes 10485760}}]

  ;(prn "!!!!!!!!!!!!!!!!!!!! max bytes " max-bytes)
  (debug "topic " topics "min-bytes " min-bytes " max-bytes " max-bytes " max-wait-time " max-wait-time)

  (-> buff
      (.writeInt (int -1))
      (.writeInt (int max-wait-time))
      (.writeInt (int min-bytes))
      (.writeInt (int (count topics))))                     ;write topic array count

  (doseq [[topic partitions] topics]
    (s/validate schemas/TOPIC-SCHEMA topic)
    (s/validate schemas/PARTITIONS-OFFSET-SCHEMA partitions)

    (-> buff
        ^ByteBuf (write-short-string topic)
        (.writeInt (count partitions)))                     ;write partition array count
    (doseq [{:keys [partition offset]} partitions]          ;default max-bytes 500mb
      (-> buff
          (.writeInt (int partition))
          (.writeLong offset)
          (.writeInt (int max-bytes)))))

  buff)


(defonce ^AtomicInteger correlation-id-counter (AtomicInteger.))

(defn get-unique-corr-id []
  (let [v (.getAndIncrement correlation-id-counter)]
    (if (= v (Integer/MAX_VALUE))                           ;check overflow
      (.set correlation-id-counter 0))
    v))


(defn ^ByteBuf write-fetch-request-header
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => FetchRequestMessage

  topics = [[\"<topic-name>\" [{:partition <partition>, :offset <offset-to-fetch-from>}]]]
  "
  [^ByteBuf buff topics {:keys [client-id] :or {client-id "1"} :as state}]
  (debug "write-fetch-request-header using state conf : " (:conf state))

  (-> buff
      (.writeShort (short API_KEY_FETCH_REQUEST))
      (.writeShort (short API_VERSION))
      (.writeInt (int (get-unique-corr-id)))                ;the correlation id must always be unique
      ^ByteBuf (write-short-string client-id)
      ^ByteBuf (write-fecth-request-message topics (:conf state))))

(defn ^ByteBuf write-fetch-request
  "
    topics = [[\"<topic-name>\" [{:partition <partition>, :offset <offset-to-fetch-from>}]]]
  "
  [^ByteBuf buff topics state]
  {:pre [topics]}
  "Writes a fetch request api call"
  (with-size buff write-fetch-request-header topics state))


(defn write-offset-request-message [^ByteBuf buff {:keys [topics max-offsets] :or {max-offsets 100000}}]
  "
	OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
	  ReplicaId => int32
	  TopicName => string
	  Partition => int32
	  Time => int64
	  MaxNumberOfOffsets => int32

	  Important: use-earliest is not used here and thus all offsets are returned up to the max-offsets default 100000
	"
  (.writeInt buff (int -1))                                 ;replica id
  (.writeInt buff (int (count topics)))                     ;write topic array count
  (doseq [[topic partitions] topics]
    (write-short-string buff topic)
    (.writeInt buff (int (count partitions)))
    (doseq [{:keys [partition]} partitions]
      (do
        (-> buff
            (.writeInt (int partition))
            (.writeLong -1)
            (.writeInt (int max-offsets))))))

  buff)


(defn write-offset-request [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"} :as state}]
  (-> buff
      (.writeShort (short API_KEY_OFFSET_REQUEST))          ;api-key
      (.writeShort (short API_VERSION))                     ;version api
      (.writeInt (int correlation-id))                      ;correlation id
      (write-short-string client-id)
      (write-offset-request-message state)))


(defn _read-offset-response [^ByteBuf in]
  "
  RequestOrResponse => Size (RequestMessage | ResponseMessage) ;;should be read by the calling message
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
  (let [;_ (.readInt in)                                ;request size int
        _ (.readInt in)                                     ;correlation id int
        topic-count (.readInt in)]                          ;topic array count int
    (doall                                                  ;we must force the operation here
      (for [_ (range topic-count)]
        (let [topic (read-short-string in)]                 ;topic name has len=short string bytes
          {:topic      topic
           :partitions (let [partition-count (.readInt in)] ;read partition array count int
                         (doall
                           (for [_ (range partition-count)]
                             {:partition  (.readInt in)     ;read partition int
                              :error-code (.readShort in)   ;read error code short
                              :offsets
                                          (let [offset-count (.readInt in)]
                                            (doall
                                              (for [_ (range offset-count)]
                                                (.readLong in))))}))) ;read offset long
           })))))

(defn send-recv-offset-request
  "Send an offset request and read the response"
  [metadata-connector host-address topics]
  {:pre [metadata-connector
         (:host host-address)
         (:port host-address)
         ;(s/validate [[s/Str [{:partition s/Int}]]] topics)
         ]}

  ;;[["1487196829664" ({:partition 0})]]

  "topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]"
  (let [timeout (get (:conf metadata-connector) :fetch-tcp-timeout 10000)
        ^ByteBuf buff (Unpooled/buffer)
        _ (do (with-size buff write-offset-request (merge (:conf metadata-connector) {:topics topics})))

        resp-bts (tcp-driver/send-f (:driver metadata-connector)
                                    host-address
                                    (fn [conn]
                                      (locking conn
                                        (let [_ (tcp-stream/write-bytes conn (Util/toBytes buff))
                                              _ (tcp-stream/flush-out conn)

                                              msg-len (tcp-stream/read-int conn timeout)
                                              resp-bts (tcp-stream/read-bytes
                                                         conn
                                                         msg-len
                                                         timeout)]
                                          resp-bts)))
                                    timeout)

        kafka-resp (_read-offset-response (Unpooled/wrappedBuffer ^"[B" resp-bts))]

    (debug "send-recv-offset-request kafka-resp " kafka-resp)
    kafka-resp))

