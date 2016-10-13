(ns
  ^{:doc "test the failure scenario where we are sending data to a cluster of 3
          one node falls off and we consume at the other end.
          The end result must be that no messages are lost"}
  kafka-clj.failure-tests
  (:require [kafka-clj.test-utils :as test-utils]
            [kafka-clj.client :as client]
            [kafka-clj.consumer.node :as consumer-node])
  (:import (com.google.common.util.concurrent RateLimiter))
  (:use midje.sweet))


(defn startup
  "Start a 3 node kafka cluster, redis instance and zookeeper instance
   Returns the resource map {:kafka {} :redis {}}"
  [topic]
  (let [res (test-utils/startup-resources 3)]
    (test-utils/create-topics res [topic] 3 2)
    res))

(defn fail-broker
  "Shutdown a random kafka server"
  [res]
  (prn "Fail broker")
  (test-utils/shutdown-random-kafka res)
  (prn "Failed broker"))

(defn shutdown
  "Shutdown all resources, the kafka cluster, redis instance and zookeeper instance"
  [res]
  (test-utils/shutdown-resources res))

(defn create-connector
  "Creates a connector to send messages to kafka"
  [res]
  (client/create-connector (test-utils/conf-brokers res) {:acks 1 :flush-on-write true :codec 2}))

(defn uniq-name [] (str (System/currentTimeMillis)))

(defn create-node [res topic]
  (consumer-node/create-node!
    {:bootstrap-brokers (test-utils/conf-brokers res)
     :redis-conf        {:host       "localhost"
                         :port       (get-in res [:redis :port])
                         :max-active 5 :timeout 1000 :group-name (uniq-name)}
     :conf              {:use-earliest        true
                         :work-calculate-freq 200}}
    [topic]))

(defn send-messages
  "Sends msg-count messages to topic, uses a rate limiter to limit at 3K per second
  connector kafka-clj.client connector
  msg-count number
  topic
  rate number upper rate at which messages should be sent
  msg-gen-f function that takes an integer and returns a byte array"
  [connector msg-count topic rate msg-gen-f]
  {:pre [connector (number? msg-count) (number? rate) (string? topic)]}
  (let [rate-limiter (RateLimiter/create (double rate))]
    (prn "sending messages")
    (dotimes [i msg-count]
      (.acquire ^RateLimiter rate-limiter)
      (client/send-msg connector topic (msg-gen-f i)))
    (prn "stop sending messages")))

(defn with-resources [topic f]
  (let [res (startup topic)]
    (try
      (f res topic)
      (finally
        (shutdown res)))))

(defn timeout? [curr-ts timeout]
  (> (- (System/currentTimeMillis) (long curr-ts)) (long timeout)))

(defn- read-messages [node expected-count timeout]
  (loop [msgs [] ts (System/currentTimeMillis)]
    (if (or (timeout? ts timeout)
            (= (count msgs) expected-count))
      (if-let [msg (consumer-node/read-msg! node timeout)]
        (recur (conj msgs msg) (System/currentTimeMillis))
        (recur msgs ts))
      msgs)))

(defn test-send-consume-with-failure
  "Runs the follwing test:
    1. create a 3 node kafka cluster and test topic with 1 partition and 2 replicas
    2.a connect and consume messages, add to counter when a message is received
    2.a connect and send messages in sub thread
    2.b sleep 15 seconds then fail a random broker
    3. wait for sending to complete, timeout on 60 seconds
    4. wait for consumption counter to hit the expected count, timeout on 60 seconds"
  [topic]
  (with-resources topic (fn [res topic]
                          (let [connector (create-connector res)
                                node (create-node res topic)
                                msg-count 100000
                                msg-send-f (future (send-messages connector msg-count topic 3000.0 #(.getBytes (str "TestMSG-" %))))]

                            ;wait for sending to cease
                            (deref msg-send-f 60000 nil)

                            (client/close connector)
                            (Thread/sleep 10000)

                            (fail-broker res)

                            (prn "waiting for message consumption")
                            (let [msg-read-count (count (read-messages node msg-count (* 60000 5)))]
                              (prn "!!!!!!!!!!############+++++++++=======>>>>>> messages " msg-read-count)
                              (consumer-node/shutdown-node! node)
                              (facts "Check that all messages were read"
                                     msg-read-count => msg-count))))))


(facts "Test send consume with one node failing"
       (test-send-consume-with-failure "data1"))
