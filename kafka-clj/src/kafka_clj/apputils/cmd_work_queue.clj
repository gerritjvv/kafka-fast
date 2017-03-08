(ns kafka-clj.apputils.cmd-work-queue
  (:require [kafka-clj.redis.protocol :as redis-api]
            [kafka-clj.redis.core :as redis-core]))


(defn read-redis-queue [[host from limit & {:strs [password] :or {password nil}}]]

  (let [redis-conn (redis-core/create {:host host :password password})
        data (redis-core/wcar redis-conn (redis-api/-lrange redis-conn "etl-kafka-work-queue" from limit))]

    (doseq [item data]
      (prn item))))
