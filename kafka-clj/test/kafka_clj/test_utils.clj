(ns kafka-clj.test-utils
  (:require [clojure.string :as cljstr])
  (:import [redis.embedded RedisServer]
           [kafka_clj.util EmbeddedKafkaCluster EmbeddedZookeeper]
           [java.util Properties]))

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

(defn startup-kafka []
  (let [zk (doto (EmbeddedZookeeper. 2181) .startup)
        kafka (doto (EmbeddedKafkaCluster. (.getConnection zk) (Properties.) [(int -1) (int -1)]) .startup)]
  {:zk zk
   :kafka kafka
   :brokers (brokers-as-map (.getBrokerList kafka))}))


(defn shutdown-kafka [{:keys [zk kafka]}]
  (.shutdown ^EmbeddedKafkaCluster kafka)
  (.shutdown ^EmbeddedZookeeper zk))

(defn startup-redis []
  (let [redis (doto (RedisServer. (int 6379)) .start)]
    {:server redis :port 6379}))

(defn shutdown-redis [{:keys [^RedisServer server]}]
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
