(ns kafka-clj.integration-v2-consumer-tests
  (:require [kafka-clj.consumer.work-organiser :refer [create-organiser! calculate-new-work get-queue-data]]
            [kafka-clj.consumer.consumer :refer [consumer-start do-work-unit!]]
            [clojure.tools.logging :refer [info]]
            [clojure.edn :as edn])
  (:use midje.sweet))


(def config (edn/read-string (slurp (str (System/getProperty "user.home") "/" ".integration/kafka-clj/conf.edn"))))

(def bootstrap-brokers (:bootstrap-brokers config))
(def topic (:topic config))
(def redis-conf (:redis-conf config))



(facts "Test Create Work Units and Consumer"
  
  (fact "Test one"
    
    (let [org (create-organiser! {:bootstrap-brokers [{:host "localhost" :port 9092}]
                                 :redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
          ]
      
      (calculate-new-work org ["ping"])
      (let [consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
            res (do-work-unit! consumer (fn [state status resp-data] (assoc state :resp-data resp-data)))
            resp-data (:resp-data res)]
        (nil? resp-data) => false
        (:status res) => :ok
        
        (apply max (map :offset resp-data)) => (apply max (map #(-> % :resp-data :offset-read) (get-queue-data org "complete")))
        
        ))))
