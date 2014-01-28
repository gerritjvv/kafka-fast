(comment
(ns kafka-clj.util
  
  (import [java.io IOException]
          [java.util Properties]
          [kafka.server KafkaConfig]
          [kafka.server KafkaServerStartable]
          [kafka.utils TestUtils]
          [org.apache.curator.test TestingServer]))


(defn- get-kafka-config [zk-connect-str]
  (let [props (-> (TestUtils/createBrokerConfigs 1) .iterator .next)]
    (.put props "zookeeper.connect" zk-connect-str)
    (KafkaConfig. props)))

(defn kafka-broker [{:keys [kafka-server]}]
  (String/format "localhost:%d"
                  (-> kafkaServer .serverConfig .port)))
  
(defn zk-connect [{:keys [zk-server]}]
  (.getConnectString zk-server))

(defn kafka-port [{:keys [kafka-server]}]
  (-> kafka-server .serverConfig .port))

(defn shutdown [{:keys [kafka-server zk-server]}]
  (.shutdown kafka-server)
  (.stop zk-server))

(defn start-test-kafka []
  (let [zk-server (TestingServer.)
        kafka-server (KafkaServerStartable. (get-kafka-config (.getConnectString zkServer)))]
    (.startup kafka-server)
    {:kafka-server kafka-server :zk-server zk-server})))
     
  