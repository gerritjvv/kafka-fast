(ns kafka-clj.integration-tests
  (:require [kafka-clj.consumer :refer [consumer read-msg shutdown-consumer]]
            [kafka-clj.client :refer [create-connector close send-msg]]
            [clojure.tools.logging :refer [info]]
            [clojure.edn :as edn])
  (:use midje.sweet))


(def config (edn/read-string (slurp (str (System/getProperty "user.home") "/" ".integration/kafka-clj/conf.edn"))))

(def brokers (:brokers config))
(def topic (:topic config))
(def redis (:redis config))

(prn "Using brokers " brokers " with topic " topic)


(defn create-consumer []
  (consumer brokers [topic]
    {:use-earliest true :max-bytes 50 :metadata-timeout 1000 :redis-conf {:redis-host redis :heart-beat 10} }))

(defn lazy-ch [c]
  (lazy-seq (cons (read-msg c) (lazy-ch c))))

(defn create-consume-seq [c]
  (flatten (lazy-ch c)))
  
(defn create-producer []
  (create-connector brokers {}))


(defn send-kafka [conn msgs]
  (doseq [msg msgs]
    (send-msg conn topic msg)))

(defn get-msgs [conn n]
  (info "get msgs ")
  (take n (create-consume-seq conn)))

(facts "consume and produce integration tests"
  (fact "test config"
       (let [msgs (doall (map #(.getBytes %) (map str (range 100))))
             producer (create-producer)
             c (create-consumer)]
         (future (send-kafka producer msgs))
         
         ;; wait and consume the messages
         (let [kafka-msgs (get-msgs c (count msgs))
               grouped (group-by :partition kafka-msgs)]
           (doseq [[_ p-msgs] grouped]
             (doseq [[offset cnt] (frequencies (map :offset p-msgs))]
               cnt => 1)))
             
         ;(map #(-> % :bts (String.)) (get-msgs c (count msgs))) => (map #(String. %) msgs)
         ;(close producer)
         ;(shutdown-consumer c)
         
    )))