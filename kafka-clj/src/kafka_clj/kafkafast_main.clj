(ns kafka-clj.kafkafast-main
  (:require [kafka-clj.redis.core :as redis-core]
            [kafka-clj.redis.protocol :as redis-api])
  (:gen-class))


(defn read-redis-queue [[host from limit & _]]

  (let [redis-conn (redis-core/create {:host host})
        data (redis-core/wcar redis-conn (redis-api/-lrange redis-conn "etl-kafka-work-queue" from limit))]

    (doseq [item data]
      (prn item))))

(defn -main [& args]
  (let [[cmd & cmd-args] args]

    (read-redis-queue cmd-args)))
