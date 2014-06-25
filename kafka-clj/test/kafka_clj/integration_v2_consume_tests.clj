(ns kafka-clj.integration-v2-consume-tests
  (:require [kafka-clj.consumer.consumer :refer [publish-work consume! close-consumer!]]
            [kafka-clj.consumer.work-organiser :refer [get-queue-data]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.async :refer [alts!! chan timeout]]
            [clojure.tools.logging :refer [info]]
            [clojure.edn :as edn])
  (:use midje.sweet))


(def config (edn/read-string (slurp (str (System/getProperty "user.home") "/" ".integration/kafka-clj/conf.edn"))))

(def bootstrap-brokers (:bootstrap-brokers config))
(def topic (:topic config))
(def redis-conf (:redis-conf config))


;=========== Requirements ===========
;===== This test requires a running kafka cluster and a redis server
;===== It also requires a topic with name "ping" and it must have at least a 100 messages published
;====================================

(facts "Test Create Work Units and Consumer"

  (fact "Test organiser and wait-and-do-work-unit!"

        (let [ts (System/currentTimeMillis)
              msgs 100
              msg-ch (chan 1000)
              consumer-conf {:consume-step 10 :redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}
              redis-conf (:redis-conf consumer-conf)
              redis-conn {:pool {:max-active (get redis-conf :max-active 20)}
                          :spec {:host  (get redis-conf :host "localhost")
                                 :port    (get redis-conf :port 6379)
                                 :password (get redis-conf :password)
                                 :timeout  (get redis-conf :timeout 4000)}}
              ]
          ;we must close the complete processor
          (car/wcar redis-conn
                    (car/flushall))

          (dotimes [i msgs]
            (prn "publish " i)
            (publish-work consumer-conf {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset i :len 10}))

          ;check that we have successfully published all messages to the work queue
          (count (get-queue-data consumer-conf (:work-queue consumer-conf))) => msgs

          (let [consumer (consume! (assoc consumer-conf :msg-ch msg-ch))]
            (Thread/sleep 2000)
            (dotimes [i msgs]
              (let [[v ch] (alts!! [msg-ch (timeout 1000)])]
                (prn "GOT VAL >>>>>>>>====== i " i " v = " v  " ch " ch)
                (nil? v) => false
                ))
            ;check that the queues are in a consistent state
            (count (get-queue-data consumer-conf (:complete-queue consumer-conf))) => msgs
            (count (get-queue-data consumer-conf (:work-queue consumer-conf))) => 0
            (count (get-queue-data consumer-conf (:working-queue consumer-conf))) => 0

            (close-consumer! consumer)))))
