(ns kafka-clj.integration-v2-work-events-tests
  (:require [kafka-clj.consumer.node :refer [create-node! shutdown-node! msg-seq!]]
            [kafka-clj.consumer.work-organiser :refer [get-queue-data]]
            [kafka-clj.consumer.consumer :refer [publish-work-response!]]
            [taoensso.carmine :as car]
            [kafka-clj.core :as redis]
            [clojure.core.async :refer [alts!! chan timeout <!!]]
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
;=====
;====================================

(facts "Test Create Work Units and Consumer"

       (fact "Test organiser and wait-and-do-work-unit!"

             (let [ts (System/currentTimeMillis)
                   consumer-conf {:bootstrap-brokers [{:host "localhost" :port 9092}] :consume-step 10 :redis-conf {:host "localhost" :max-active 5 :timeout 1000 :group-name "test"} :conf {}}
                   redis-conf (:redis-conf consumer-conf)
                   node (create-node! consumer-conf ["ping"])
                   redis-conn (get-in node [:org :redis-conn])
                   event-ch (:work-unit-event-ch node)
                   wu {:topic "ping" :offset 10 :status "ok"}
                   ]

               event-ch => truthy

               (publish-work-response! (:conf node)  wu "ok" {:offset-read 10})

               ;check queues, we should have zero in work, working and complete queues
               ;its expected that the node should have processed all work
               (let [[event ch] (alts!! [event-ch (timeout 1000)])]
                 event => truthy
                 (:event event) => "done"
                 (number? (:ts event)) => true
                 (:wu event) => truthy
                 (-> event :wu :topic) => truthy)
               (shutdown-node! node)

               )))
