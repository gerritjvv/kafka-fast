(ns kafka-clj.integration-v2-consumer-tests
  (:require [kafka-clj.consumer.work-organiser :refer [create-organiser! close-organiser! calculate-new-work get-queue-data]]
            [kafka-clj.consumer.consumer :refer [consumer-start do-work-unit! wait-and-do-work-unit!]]
            [taoensso.carmine :as car :refer [wcar]]
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

(facts "Test Create Work Units and Consumer"
  
  (fact "Test organiser and wait-and-do-work-unit!"

        (let [ts (System/currentTimeMillis)
              org (create-organiser! {:bootstrap-brokers [{:host "localhost" :port 9092}]
                                      :redis-conf {:host "localhost" :max-active 1 :timeout 500} :working-queue (str "working" ts) :complete-queue (str "complete" ts) :work-queue (str "work" ts) :conf {}})
              redis-conn (:redis-conn org)
              ]
          ;we must close the complete processor
          (.shutdownNow (:work-complete-processor-future org))
          (car/wcar redis-conn
                    (car/flushall))
          (do (prn ">>>>>>>> !!!!!!!!!!!!!!!!! FUCK-1"))

          (calculate-new-work org ["ping"])
          (do (prn ">>>>>>>> !!!!!!!!!!!!!!!!! FUCK0"))

          (let [_ (do (prn ">>>>>>>> !!!!!!!!!!!!!!!!! FUCK1"))

                consumer (consumer-start {:redis-conf {:host "localhost" :max-active 1 :timeout 500} :working-queue (:working-queue org) :complete-queue (:complete-queue org) :work-queue (:work-queue org) :conf {}})
                _ (do (prn ">>>>>>>> !!!!!!!!!!!!!!!!! FUCK2"))
                res (wait-and-do-work-unit! consumer (fn [state status resp-data] (assoc state :resp-data resp-data)))
                resp-data (:resp-data res)]
            (close-organiser! org)
            (nil? resp-data) => false
            (:status res) => :ok
            (prn "res = " res)
            (prn "complete queue data " (get-queue-data org (:complete-queue org)))
            (apply max (map :offset resp-data)) => (apply max (map #(-> % :resp-data :offset-read) (get-queue-data org (:complete-queue org))))
            ))))

;@TODO create a work complete processor test