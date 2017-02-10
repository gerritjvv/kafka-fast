(ns kafka-clj.produce
  (:require [kafka-clj.codec :refer [crc32-int compress]]
            [kafka-clj.response :as kafka-resp]
            [clojure.tools.logging :refer [error info debug]]
            [kafka-clj.buff-utils :refer [write-short-string with-size compression-code-mask]]
            [clj-tuple :refer [tuple]]
            [kafka-clj.msg-persist :refer [cache-sent-messages create-send-cache]]
            [kafka-clj.tcp :as tcp]
            [kafka-clj.protocol :as protocol])
  (:import
    (java.net Socket SocketException)
    [java.util.concurrent.atomic AtomicLong AtomicInteger]
    [io.netty.buffer ByteBuf Unpooled ByteBufAllocator UnpooledByteBufAllocator PooledByteBufAllocator]
    [kafka_clj.util Util]
    (java.io ByteArrayInputStream DataInputStream)
    (java.util Arrays)))

(defonce ^:constant API_KEY_PRODUCE_REQUEST protocol/API_KEY_PRODUCE_REQUEST)
(defonce ^:constant API_KEY_FETCH_REQUEST protocol/API_KEY_FETCH_REQUEST)
(defonce ^:constant API_KEY_OFFSET_REQUEST protocol/API_KEY_OFFSET_REQUEST)
(defonce ^:constant API_KEY_METADATA_REQUEST protocol/API_KEY_METADATA_REQUEST)
(defonce ^:constant API_KEY_SASL_HANDSHAKE protocol/API_KEY_SASL_HANDSHAKE)


(defonce ^:constant API_VERSION protocol/API_VERSION)

(defonce ^:constant MAGIC_BYTE protocol/MAGIC_BYTE)


(defrecord Producer [client host port])
(defrecord Message [topic partition ^bytes bts])


(defn flush-on-byte->fn
  "Returns a check-f for buffered-chan that will flush if the accumulated byte count is bigger than that of byte-limit"
  [^long byte-limit]
  (fn
    ([] 0)
    ([^long byte-cnt msg]
     (let [total-cnt (+ byte-cnt (count (:bts msg)))]
       (if (>= total-cnt byte-limit)
         (tuple true 0)
         (tuple false total-cnt))))))

(defn shutdown [{:keys [client]}]
  (when client
    (try
      (tcp/close! client)
      (catch Exception e (error e "Error while shutting down producer")))))

(defn message [topic partition ^bytes bts]
  (Message. topic partition bts))


(defn write-message [^ByteBuf buff codec ^bytes bts]
  (let [
        pos (.writerIndex buff)
        ]
    (-> buff
        (.writeInt (int -1))                                ;crc32
        (.writeByte (byte 0))                               ;magic
        (.writeByte (int (bit-or (byte 0) (bit-and compression-code-mask codec)))) ;attr
        (.writeInt (int -1))                                ;nil key
        (.writeInt (int (count bts)))                       ;value bts len
        (.writeBytes bts)                                   ;value bts

        )
    (let [arr (byte-array (- (.writerIndex buff) pos 4))]
      (.getBytes buff (+ pos 4) arr)
      (Util/setUnsignedInt buff (int pos) (crc32-int arr)))

    ))


(defn write-message-set
  "Writes a message set and returns a tuple of [first-offset msgs]"
  [^ByteBuf buff correlation-id codec msgs]
  (loop [msgs1 msgs]
    (if-let [msg (first msgs1)]
      (do
        (-> buff
            (.writeLong 0)                                  ;offset
            (with-size write-message codec (:bts msg))      ;writes len message
            )
        (recur (rest msgs1))
        )
      ))
  (tuple correlation-id msgs))


(defn write-compressed-message-set
  "Writes the compressed message and returns a tuple of [offset msgs]"
  [^ByteBuf buff correlation-id codec msgs]
  ;use the buffer allocator to get a new buffer
  (let [^ByteBufAllocator alloc (.alloc buff)
        msg-buff (.buffer alloc)]

    (try
      (let [_ (write-message-set msg-buff correlation-id 0 msgs) ;write msgs to msg-buff
            arr (byte-array (- (.writerIndex msg-buff) (.readerIndex msg-buff)))]
        (.readBytes msg-buff arr)

        (-> buff
            (.writeLong 0)                                  ;offset
            (with-size write-message codec
                       (compress codec arr)
                       ))
        (tuple correlation-id msgs))                        ;return the (tuple offset msgs) of the uncompressed messages but with the compressed offset
      (finally                                              ;all ByteBuff are reference counted
        (.release msg-buff)
        ))))


(defn write-request
  "Writes the messages and return a sequence of [ [offset msgs] ... ] The offset is the first offset in the message set
  For compressed messages this is the uncompressed message, and allows us to retry message sending."
  [^ByteBuf buff {:keys [client-id codec acks timeout] :or {client-id "1" codec 0 acks 1 timeout 1000} :as conf}
   msgs]
  (let [correlation-id (protocol/unique-corrid!)]
    (-> buff
        (.writeShort (short API_KEY_PRODUCE_REQUEST))       ;api-key
        (.writeShort (short API_VERSION))                   ;version api
        (.writeInt (int correlation-id))                    ;correlation id
        (write-short-string client-id)                      ;short + client-id bytes
        (.writeShort (short acks))                          ;acks
        (.writeInt (int timeout)))                          ;timeout

    (let [topic-group (group-by :topic msgs)]
      (.writeInt buff (int (count topic-group)))            ;topic count
      (doall
        (apply concat
               (for [[topic topic-msgs] topic-group]
                 (do
                   (write-short-string buff topic)          ;short + topic bytes
                   (let [partition-group (group-by :partition topic-msgs)]
                     (.writeInt buff (int (count partition-group))) ;partition count
                     (for [[partition partition-msgs] partition-group]
                       (do (.writeInt buff (int partition)) ;partition

                           ;TODO we have duplicate messages here

                           (if (= codec 0)
                             (with-size buff write-message-set correlation-id codec msgs)
                             (with-size buff write-compressed-message-set correlation-id codec msgs))))))))))))


(defn read-response [{:keys [client]} timeout]
  (prn "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!READ RESPONSE")
  (let [{:keys [^Socket socket ^DataInputStream input]} client]
    (when (not (.isClosed socket))
      (loop [start-ts (System/currentTimeMillis)]
        (if (> (.available input) 4)
          (let [size (.readInt input)
                bts (byte-array size)
                _ (.read input bts)
                btsIn (DataInputStream. (ByteArrayInputStream. bts))]
            (prn "resp " (Arrays/toString bts))
            (try
              (flatten (kafka-resp/in->kafkarespseq btsIn))
              (finally
                (.close btsIn))))
          (when (<= (- (System/currentTimeMillis) start-ts) timeout)
            (recur start-ts)))))))

(defn- write-message-for-ack
  "Writes the messages to the buff and send the results of [[offset msgs] ...] to the cache.
   This function always returns the msgs"
  [connector conf msgs ^ByteBuf buff]
  (if buff (cache-sent-messages connector (with-size buff write-request conf msgs)))
  msgs)

;this is only used with the single message producer api where ack > 0
(def global-message-ack-cache (delay (create-send-cache {})))

(defn send-messages
  "Send messages by writing them to the tcp client.
  The write is async.
  If the conf properties acks is > 0 the messages will also be written to an inmemory cache,
  the cache expires and has a maximum size, but it allows us to retry failed messages."
  ([producer conf msgs]
    (send-messages {:send-cache @global-message-ack-cache} producer conf msgs))
  ([connector
    {:keys [client]}
    {:keys [acks] :or {acks 0} :as conf}
    msgs]
    (let [byte-buff (Unpooled/buffer)]
      (if (> acks 0)
        (write-message-for-ack connector conf msgs byte-buff)
        (with-size byte-buff write-request conf msgs))

      (if (:flush-on-write conf)
        (tcp/write! client byte-buff :flush true)
        (tcp/write! client byte-buff)))))

(defn producer-closed? [producer]
  (tcp/closed? (:client producer)))

(defn producer
  "returns a producer for sending messages, the decoder is a producer-response-decoder"
  ([host port]
    (producer host port {}))
  ([host port conf]
    (try
      (let [c (tcp/tcp-client host port conf)]

        (info "Creating client instance " host ":" port)
        (->Producer c host port))
      (catch Exception e (.printStackTrace e)))))



;; ------- METADATA REQUEST API

(defn write-metadata-request
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    ApiKey => int16
    ApiVersion => int16
    CorrelationId => int32
    ClientId => string
    MetadataRequest => [TopicName]
       TopicName => string
   "
  [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"}}]
  (-> buff
      (.writeShort (short API_KEY_METADATA_REQUEST))        ;api-key
      (.writeShort (short API_VERSION))                     ;version api
      (.writeInt (int correlation-id))                      ;correlation id
      (write-short-string client-id)                        ;short + client-id bytes
      (.writeInt (int 0))))                                 ;write empty topic, dont use -1 (this means nil), list to receive metadata on all topics


(defn send-metadata-request
  "Writes out a metadata request to the producer's client"
  [{:keys [client]} conf]
  (let [buff (Unpooled/buffer)
        _ (do (with-size buff write-metadata-request conf))]
    (tcp/write! client buff :flush true)))


(defn metadata-request-producer
  "Returns a producer with a metadata-response-decoder set"
  [host port conf]
  (if (not host)
    (throw (RuntimeException. (str "Nill host is not allowed here"))))

  (let [c (tcp/tcp-client host port conf)]

    (debug ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> creating conn " c)
    (->Producer c host port)))
      
