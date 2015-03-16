(ns kafka-clj.redis.core
  (:refer-clojure :exclude [get set])
  (:require [kafka-clj.redis.protocol :refer :all]
            [kafka-clj.redis.single :as redis-single]))



(defn flushall [pool] (-flushall pool))

(defn lpush [pool queue obj] (-lpush pool queue obj))
(defn llen [pool queue] (-llen pool queue))
(defn lrem [pool queue n obj] (-lrem pool queue n obj))
(defn get [pool k] (-get pool k))
(defn set [pool k v] (-set pool k v))
(defn lrange [pool q n limit] (-lrange pool q n limit))
(defn brpoplpush [pool queue queue2 n] (-brpoplpush pool queue queue2 n))
(defn acquire-lock [pool lock-name timeout-ms wait-ms] (-acquire-lock pool lock-name timeout-ms wait-ms))
(defn release-lock [pool lock-name owner-uuid] (-release-lock pool lock-name owner-uuid))
(defn have-lock? [pool lock-name owner-uuid] (-have-lock? pool lock-name owner-uuid))
(defn close! [pool] (-close! pool))



(defmacro wcar [pool & body]
  `(-wcar ~pool #(do
                 ~@body)))

(defmacro with-lock
  "Attempts to acquire a distributed lock, executing body and then releasing
  lock when successful. Returns {:result <body's result>} on successful release,
  or nil if the lock could not be acquired. If the lock is successfully acquired
  but expires before being released, throws an exception."
  [pool lock-name timeout-ms wait-ms & body]
  `(let [pool# ~pool]                                       ;pool lock-name timeout-ms wait-ms
     (when-let [uuid# (acquire-lock pool# ~lock-name ~timeout-ms ~wait-ms)]
       (try
         {:result (do ~@body)} ; Wrapped to distinguish nil body result
         (catch Throwable t# (throw t#))
         (finally
           (when-not (release-lock pool# ~lock-name uuid#)
             (throw (ex-info (str "Lock expired before it was released: "
                                  ~lock-name)
                             {:lock-name ~lock-name}))))))))


(defn redis-conn
  "Creates a redis connection for a single redis instance"
  [spec opts]
  (redis-single/create spec opts))

(defn redis-cluster-conn
  "Creates a redis connection for a redis cluster"
  [& args]
  (throw (RuntimeException. "Not supported yet")))