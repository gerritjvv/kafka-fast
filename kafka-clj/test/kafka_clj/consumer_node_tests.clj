(ns kafka-clj.consumer-node-tests
  (:require [kafka-clj.test-utils :refer [startup-redis shutdown-redis]]
            [kafka-clj.redis.core :as redis]
            [kafka-clj.consumer.node :refer [copy-redis-queue]])
  (:use midje.sweet))


;;; Internal tests for kafka-clj.consumer.node


(defonce system (atom nil))

(defn setup []
   (let [redis (startup-redis)
         redis-conn (redis/create {:host "localhost" :port (:port redis)})]

     (reset! system {:redis redis :redis-conn redis-conn})))

(defn teardown [{:keys [redis]}]
  (when redis (shutdown-redis redis)))

(with-state-changes
  [(before :facts (setup)) (after :facts (teardown @system))]
  (fact "Test copy queue"
        (let [redis-conn (:redis-conn @system)
              q1 (str "q_" (System/nanoTime))
              q2 (str "q_" (System/nanoTime))]
          (redis/wcar redis-conn
                      ;important to note maps must be sent sorted-map
                      (redis/lpush* redis-conn q1 (map #(sorted-map :i %) (range 200))))
          (copy-redis-queue redis-conn q1 q2)
          (redis/wcar redis-conn
                      (redis/llen redis-conn q1)
                      (redis/llen redis-conn q2)) => [0 200])))

