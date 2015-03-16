(ns kafka-clj.integration-v2-work-complete-tests
  (:require [kafka-clj.consumer.work-organiser :refer [create-organiser! close-organiser! calculate-new-work get-queue-data]]
            [kafka-clj.consumer.consumer :refer [consumer-start do-work-unit! wait-and-do-work-unit!]]
            [kafka-clj.redis.core :as redis]
            [clojure.tools.logging :refer [info]]
            [clojure.edn :as edn])
  (:use midje.sweet))


(def config (edn/read-string (slurp (str (System/getProperty "user.home") "/" ".integration/kafka-clj/conf.edn"))))

(def bootstrap-brokers (:bootstrap-brokers config))
(def topic (:topic config))
(def redis-conf (:redis-conf config))


;=========== Requirements ===========
;===== This test requires a running kafka cluster and a redis server
;===== It also requires a topic with name "ping" and it must have at least one message published
;====================================

(defn wait-for-queue-count [redis-conn queue expected-count iterations]
  (loop [c(redis/wcar redis-conn (redis/llen queue)) i 0]
    (if (and (< c expected-count) (< i iterations))
      (let [v (redis/wcar redis-conn (redis/llen queue))]
        (prn "waiting " queue  " results are " v)
        (Thread/sleep 1000)
        (recur v  (inc i)))
      c)))

(facts "Test Create Work Units and Consumer"
       (fact "Test work-complete-processor"
             (let [ts (System/currentTimeMillis)

                   org (create-organiser! {:bootstrap-brokers [{:host "localhost" :port 9092}]
                                           :consume-step 10
                                           :redis-conf {:host "localhost" :max-active 10 :timeout 500} :working-queue (str "working" ts) :complete-queue (str "complete" ts) :work-queue (str "work" ts) :conf {}})
                   redis-conn (:redis-conn org)
                   complete-queue (:complete-queue org)
                   work-queue (:work-queue org)
                   working-queue (:working-queue org)
                   ]
               (redis/wcar redis-conn
                         (redis/flushall))
               (redis/wcar redis-conn
                         (redis/lpush complete-queue {:resp-data {:offset-read 10} :offset 0 :len 10}))

               (let [[complete work working]
                     (redis/wcar redis-conn
                               [(redis/lrange complete-queue 0 -1)
                                (redis/lrange work-queue 0 -1)
                                (redis/lrange working-queue 0 -1)
                                ])
                     ]
                 (count complete) => 0
                 (count work) => 0
                 (count working) => 0)


               ;test work fail processor
               (redis/wcar redis-conn
                         (redis/flushall))
               (redis/wcar redis-conn
                         (redis/lpush complete-queue {:status :fail :resp-data {:offset-read 10} :offset 0 :len 10}))


               (wait-for-queue-count redis-conn work-queue 1 10)
               (let [[complete work working :as m]
                     (redis/wcar redis-conn
                               [(redis/lrange complete-queue 0 -1)
                                (redis/lrange work-queue 0 -1)
                                (redis/lrange working-queue 0 -1)
                                ])
                     ]
                 (prn "TEST REST: " m)                      ;﻿ [[] [] [{:offset 0, :resp-data {:offset-read 10}, :status :fail, :len 10}]]


                 (count complete) => 0
                 (count work) => 0
                 (count working) => 1)
(comment

  ;test work half done
  (redis/wcar redis-conn
              (redis/flushall))
  (redis/wcar redis-conn
              (redis/lpush complete-queue {:status :ok :resp-data {:offset-read 5} :offset 0 :len 10}))

  (Thread/sleep 3000)
  (let [[complete work working]
        (redis/wcar redis-conn
                    [(redis/lrange complete-queue 0 -1)
                     (redis/lrange work-queue 0 -1)
                     (redis/lrange working-queue 0 -1)
                     ])
        ]
    (count complete) => 0
    (count work) => 1
    (first work) => {:status :ok, :len 5, :offset 6} ;we test that a new work unit is assigned with the remaining offset
    (count working) => 0))

               (prn "END")
               (close-organiser! org)
               )))
