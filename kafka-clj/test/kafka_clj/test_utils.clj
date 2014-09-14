(ns kafka-clj.test-utils
  (:require [clojure.string :as cljstr])
  (:import [redis.embedded RedisServer]
           [kafka_clj.util EmbeddedKafkaCluster EmbeddedZookeeper]
           [java.util Properties]))

(comment
  (require '[kafka-clj.client :refer [create-connector send-msg close]])
  (require '[kafka-clj.metadata :refer [get-metadata]])
  (require '[kafka-clj.consumer.util :as cutil])

  (use 'kafka-clj.test-utils)
  (def res (startup-resources "ping"))

  (def c (create-connector (get-in res [:kafka :brokers]) {}))
  (dotimes [i 100] (send-msg c "ping" (.getBytes "hi")))

  (def kafka (:kafka res))

  (def meta-data (get-metadata (-> c :bootstrap-brokers) {}))

  (cutil/get-broker-offsets {:offset-producers (ref {})} meta-data ["ping"] (get-in c [:state :conf]))

  (require '[kafka-clj.consumer.node :refer [create-node!]])
  (def node (create-node!
              {:bootstrap-brokers (get-in res [:kafka :brokers])
               :redis-conf {:host "localhost"
                            :port (get-in res [:redis :port])
                            :max-active 5 :timeout 1000 :group-name "fuck"}
               :conf {:use-earliest true
                      :work-calculate-freq 200}}
              ["ping"]))
  )
;;USAGE
;; (def state (start-up-resources))
;; (:kafka state) ;;returns kafka and zk map {:zk zk :kafka kafka :brokers brokers}
;; (:redis state) ;;returns redis map {:server :port}
;; (shutdown-resources state)

(defn- brokers-as-map
  "Takes a comma separated string of type \"host:port,hostN:port,hostN+1:port\"
   And returns a map of form {host port hostN port hostN+1 port}"
  [^String s]
  (for [pair (cljstr/split s #"[,]")
        :let [[host port] (cljstr/split pair #"[:]")]]
    {:host host :port (Integer/parseInt port)}))

(defn- startup-kafka []
  (let [zk (doto (EmbeddedZookeeper. 2181) .startup)
        kafka (doto (EmbeddedKafkaCluster. (.getConnection zk) (Properties.) [(int -1) (int -1)]) .startup)]
  {:zk zk
   :kafka kafka
   :brokers (brokers-as-map (.getBrokerList kafka))}))


(defn- shutdown-kafka [{:keys [zk kafka]}]
  (.shutdown ^EmbeddedKafkaCluster kafka)
  (.shutdown ^EmbeddedZookeeper zk))

(defn- startup-redis []
  (let [redis (doto (RedisServer. (int 6379)) .start)]
    {:server redis :port 6379}))

(defn- shutdown-redis [{:keys [^RedisServer server]}]
  (when server
    (.stop server)))



(defn create-topics [resources & topics]
  (-> resources :kafka :kafka (.createTopics topics)))

(defn startup-resources [& topics]
  (let [res {:kafka (startup-kafka)
             :redis (startup-redis)}]
    (if (not-empty topics)
      (apply create-topics res topics))
    res))

(defn shutdown-resources [{:keys [kafka redis]}]
  (shutdown-kafka kafka)
  (shutdown-redis redis))
