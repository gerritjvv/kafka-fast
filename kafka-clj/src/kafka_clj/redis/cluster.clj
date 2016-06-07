(ns kafka-clj.redis.cluster
  (:refer-clojure :exclude [get set])
  (:require [taoensso.nippy :refer [freeze thaw]]
            [kafka-clj.redis.protocol :refer [IRedis]]
            [clojure.tools.logging :refer [info error]])
  (:import [kafka_clj.util Util]
           [org.redisson.core RBucket RLock]
           [org.redisson Redisson Config ClusterServersConfig SingleServerConfig]
           [java.nio ByteBuffer]
           (java.util Queue List)
           (java.util.concurrent TimeUnit)
           (org.redisson.client.codec Codec)
           (org.redisson.client.protocol Decoder Encoder)
           (io.netty.buffer ByteBuf)))

(defprotocol IAsStr
  (-as-str [obj]))

(defprotocol IToEncodedBytes
  (-write [obj]))

(defprotocol IToRawBytes
  (-raw-bytes [obj]))

;; only use nippy for clojure objects (except True/False) for String and Number use a binary string
(extend-protocol
  IToEncodedBytes
  String (-write [obj] (Util/byteString obj))
  Number (-write [obj] (Util/byteString obj))
  nil    (-write [obj] (freeze obj))
  Object (-write [obj] (freeze obj)))

(extend-protocol
  IToRawBytes

  ByteBuffer
  (-raw-bytes [^ByteBuffer obj]
    (Util/toBytes obj))

  ByteBuf
  (-raw-bytes [^ByteBuf obj]
    (Util/toBytes obj)))


  (extend-protocol
    IAsStr

    ByteBuffer
    (-as-str [^ByteBuffer obj]
             (Util/asStr obj))

    ByteBuf
    (-as-str [^ByteBuf obj]
             (Util/asStr obj)))

(defn from-bytes [^bytes bts]
  (if (Util/isNippyCompressed bts)
    (thaw bts)
    (String. bts "UTF-8")))

(defn nippy-decoder []
  (reify Decoder
    (decode [this buffer state]
      (from-bytes (-raw-bytes buffer)))))

(defn nippy-encoder []
  (reify Encoder
    (encode [this o]
      (-write o))))

(defn nippy-map-key-decoder []
  (reify Decoder
    (decode [this buffer state]
      (-as-str buffer))))

(defn nippy-map-key-encoder []
  (reify Encoder
    (encode [this o]
      (.getBytes (str o) "UTF-8"))))


(defn codec []
  (reify Codec
    (getMapValueDecoder [this]
      (nippy-decoder))
    (getMapValueEncoder [this]
      (nippy-encoder))
    (getMapKeyDecoder [this]
      (nippy-map-key-decoder))
    (getMapKeyEncoder [this]
      (nippy-map-key-encoder))
    (getValueDecoder [this]
      (nippy-decoder))
    (getValueEncoder [this]
      (nippy-encoder))))

(defn ^Config create-config [hosts]
  (let [^Config conf (.setCodec (Config.) (codec))]
    (if (> (count hosts) 1)
      (let [^ClusterServersConfig config (.useClusterServers conf)]
        (.setScanInterval config (int 2000))
        (.addNodeAddress config (into-array String (mapv #(Util/correctURI (str %)) hosts))))
      (-> conf .useSingleServer ^SingleServerConfig (.setAddress (first hosts))))

    conf))

(defn connect
  ([host & hosts]
    (Redisson/create (create-config (conj hosts host)))))


(defn set [^Redisson cmd ^String k v]
  (->
    cmd
    ^RBucket (.getBucket k)
    (.set v)))

(defn get [^Redisson cmd ^String k]
  (->
    cmd
    ^RBucket (.getBucket k)
    .get))

(defn lpush [^Redisson cmd ^String queue v]
  (->
    cmd
    ^Queue (.getQueue queue)
    (.offer v)))

(defn llen [^Redisson cmd ^String queue]
  (-> cmd ^Queue (.getQueue queue) .size))

(defn lrem [^Redisson cmd ^String queue n v]
  (-> cmd ^List (.getList queue) (.remove v)))

(defn lrange [^Redisson cmd ^String queue ^long n ^long limit]
  (let [^List ls (.getList cmd queue)
        size (.size ls)]
    (flatten
      (into [] (.subList ls (int (if (< n 0) 0 n)) (int (if (>= limit size) (dec size) limit)))))))


(defn timeout? [^long start-time ^long timeout]
  (> (- (System/currentTimeMillis) start-time) timeout))

(defn pop-retry [^Queue cmd ^long timeout]
  (let [start-time (System/currentTimeMillis)]
    (loop [v (.poll cmd)]
      (if v
        v
        (do
          (Thread/sleep 100)
          (when-not (timeout? (long start-time) (long timeout))
            (recur (.poll cmd))))))))

(defn brpoplpush [^Redisson cmd ^String queue1 ^String queue2 ^long timeout]
  (let [q (.getQueue cmd queue1)]
    (when-let [v (pop-retry q timeout)]
      (.add ^Queue (.getQueue cmd queue2) v)
      v)))

(defn flushall [^Redisson cmd] )
(defn close! [^Redisson cmd] (.shutdown cmd))


(defn acquire-lock [^Redisson cmd ^String lock-name ^long timeout-ms ^long wait-ms]
  (let [^RLock lock (.getLock cmd lock-name)]

    (try
      (if (.tryLock lock wait-ms timeout-ms TimeUnit/MILLISECONDS) lock)
      (catch Exception e (error e e)))))

(defn release-lock [^Redisson cmd ^String lock-name ^RLock lock]
  (when lock
    (.unlock lock))
  true)

(defn have-lock? [^Redisson cmd ^String lock-name lock]
  (and lock (.isLocked ^RLock lock)))


(defrecord RedissonObj [pool]
  IRedis

  (-conn-pool-idle [_] -1)
  (-conn-pool-active [_] -1)

  (-lpush* [pool queue obj-coll]
    (doseq [obj obj-coll]
      (lpush (:pool pool) queue obj)))
  (-lpush [pool queue obj] (lpush (:pool pool) queue obj))
  (-llen  [pool queue] (llen (:pool pool) queue))
  (-lrem  [pool queue n obj] (lrem (:pool pool) queue n obj))
  (-get   [pool k] (get (:pool pool) k))
  (-set   [pool k v] (set (:pool pool) k v))
  (-lrange [pool q n limit] (lrange (:pool pool) q n limit))
  (-brpoplpush [pool queue queue2 n] (brpoplpush (:pool pool) queue queue2 n))
  (-acquire-lock [pool lock-name timeout-ms wait-ms] (acquire-lock (:pool pool) lock-name timeout-ms wait-ms))
  (-release-lock [pool lock-name owner-uuid] (release-lock(:pool pool) lock-name owner-uuid))
  (-have-lock?   [pool lock-name owner-uuid] (have-lock? (:pool pool) lock-name owner-uuid))
  (-flushall [pool] (flushall (:pool pool)))
  (-close! [pool] (close! (:pool pool)))
  (-wcar [_ f] (f)))


(defn create
  ([hosts]
   (let [sp (clojure.string/split hosts #"[ ;,]")]
     (if (> (count sp) 1)
       (apply create sp)
       (->RedissonObj (apply connect sp)))))
  ([host & hosts]
   (prn "redis-cluster/create host: " host " hosts: " hosts)
    (->RedissonObj (apply connect (conj hosts host)))))