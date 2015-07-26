(ns kafka-clj.test-utils
  (:require [clojure.string :as cljstr])
  (:import [redis.embedded RedisServer]
           [kafka_clj.util EmbeddedKafkaCluster EmbeddedZookeeper]
           [java.util Properties]
           (org.apache.log4j BasicConfigurator)))

;;USAGE
;; (def state (start-up-resources))
;; (:kafka state) ;;returns kafka and zk map {:zk zk :kafka kafka :brokers brokers}
;; (:redis state) ;;returns redis map {:server :port}
;; (shutdown-resources state)

;;setup log4j configuration
(BasicConfigurator/configure)

(defn- brokers-as-map
  "Takes a comma separated string of type \"host:port,hostN:port,hostN+1:port\"
   And returns a map of form {host port hostN port hostN+1 port}"
  [^String s]
  (for [pair (cljstr/split s #"[,]")
        :let [[host port] (cljstr/split pair #"[:]")]]
    {:host host :port (Integer/parseInt port)}))

(defn startup-kafka
  ([]
    (startup-kafka 1))
  ([nodes]
   (let [zk (doto (EmbeddedZookeeper. 2181) .startup)
         kafka (doto (EmbeddedKafkaCluster. (.getConnection zk) (Properties.) (repeat nodes (int -1))) .startup)]
     {:zk zk
      :kafka kafka
      :brokers (brokers-as-map (.getBrokerList kafka))})))

(defn shutdown-kafka [{:keys [zk kafka]}]
  (.shutdown ^EmbeddedKafkaCluster kafka)
  (.shutdown ^EmbeddedZookeeper zk))

(defn startup-redis []
  (let [redis (doto (RedisServer. (int 6379)) .start)]
    {:server redis :port 6379}))

(defn shutdown-redis [{:keys [^RedisServer server]}]
  (when server
    (.stop server)))

(defn create-topics
  ([resources topics partition replication]
   {:pre (coll? topics) (number? partition) (number? replication)}
   (.createTopics ^EmbeddedKafkaCluster (get-in resources [:kafka :kafka]) topics (int partition) (int replication))))

(defn shutdown-random-kafka [resources]
  (.shutdownRandom ^EmbeddedKafkaCluster (get-in resources [:kafka :kafka])))

(defn startup-resources
  "
  nodes number of kafka nodes to created (must be an integer)
  topics varargs of topics to create, can be empty
  Returns a map of
  {:kafka {:kafka EmbeddedKafkaCluster
           :zk EmbeddedZookeeper
           :brokers [{:host <broker> :port <port>}]
           }
   :redis EmbeddedRedis
   }"
  [nodes & topics]
  (let [^EmbeddedKafkaCluster kafka (startup-kafka nodes)
        res {:kafka kafka
             :redis (startup-redis)}]
    (when (not-empty topics)
      (create-topics res topics 1 1))
    res))

(defn shutdown-resources
  "Shutdown kafka, zookeeper and redis resources created
  res must be the map returned from startup-resources"
  [{:keys [kafka redis]}]
  (shutdown-kafka kafka)
  (shutdown-redis redis))

(defn conf-brokers
  "Get the broker map from the resources map
   res {:kafka {:brokers [{:host <broker> :port <port>}]}}
   returns [{:host <broker> :port <port>}]"
  [res]
  (get-in res [:kafka :brokers]))
