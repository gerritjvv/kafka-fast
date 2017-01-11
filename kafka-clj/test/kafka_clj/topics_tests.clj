(ns kafka-clj.topics-tests
  (:require [kafka-clj.topics :as topics]
            [kafka-clj.test-utils :as utils]
            [kafka-clj.tcp :as tcp]
            [kafka-clj.client :as client])
  (:use midje.sweet))



(defonce state (atom nil))

(defn start-resources! []
  (swap! state (constantly (utils/startup-kafka))))

(defn stop-resources! []
  (utils/shutdown-kafka @state))

(defn create-client []
  (let [{:keys [host port]} (first (:brokers @state))]
    (tcp/tcp-client host port {})))
;
(with-state-changes
  [(before :facts (start-resources!))
   (after :facts (stop-resources!))]

  (fact "Test create topic"
        (let [topic (str "topic-" (System/currentTimeMillis))
              client  (client/create-connector (:brokers @state)
                                               {:flush-on-write true
                                                :topic-auto-create true
                                                :default-replication-factor 1
                                                :default-partitions 2
                                                :acks 0})]

          ;;send a message to trigger the topic creation
          (client/send-msg client topic (.getBytes "Hello"))

          (utils/kafka-topic-exists @state topic) => truthy)))
