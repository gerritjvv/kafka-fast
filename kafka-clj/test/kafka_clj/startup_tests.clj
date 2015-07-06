(ns
  ^{:doc "Test startup conditions for when brokers are down on startup"}
  kafka-clj.startup-tests
  (:require
    [kafka-clj.test-utils :refer [startup-resources shutdown-resources create-topics]]
    [kafka-clj.consumer.node :as node]
    [kafka-clj.client :as client]
    [kafka-clj.metadata :as meta])
  (:use midje.sweet))


(defonce state-ref (ref {}))
(defonce test-topic "test")

(defonce test-range (range 100))

(defn- send-test-messages [c topic n]
  {:pre [c (string? topic) (number? n)]}
  (dotimes [i n]
    (client/send-msg c topic (.getBytes (str (take (rand-int 100) test-range) i)))))


(defn- create-node [brokers]
  {:pre [(coll? brokers)]}
  (node/create-node!
    {:bootstrap-brokers brokers
     :redis-conf        {:host       "localhost"
                         :port       (get-in @state-ref [:redis :port])
                         :max-active 5 :timeout 1000 :group-name "test"}
     :conf              {:use-earliest        true
                         :work-calculate-freq 200}}
    [test-topic]))

(defn- create-connector [brokers]
  {:pre [(coll? brokers)]}
  (client/create-connector brokers {:flush-on-write true :codec 2}))

(defn- send-test-data [brokers n]
  {:pre [(coll? brokers) (number? n)]}
  (let [conn (create-connector brokers)]
    (try
      (send-test-messages conn test-topic 100)
      (finally
        (client/close conn)))))
(defn test-facts [resources]

  (facts "Test create connector all brokers dead, expect an exception to be thrown"
         (try
           (let [brokers [{:host "hi" :port 123} {:host "abc" :port 123}]
                 _ (create-connector brokers)]

             ;;dummy test because we expect an exception
             1 => false)
           (catch Throwable t
             (do
               (:type (ex-data t)) => :metadata-exception))))

  (facts "Test create connector with one false broker"
         (let [brokers (reverse (cons {:host "bla" :port 1223} (get-in resources [:kafka :brokers])))
               conn (create-connector brokers)]
           conn => truthy
           1 => 1
           (client/close conn)
           ))

  (facts "Test create node with all brokers dead, expect an exception to be thrown"
         (try
           (let [brokers [{:host "hi" :port 123} {:host "abc" :port 123}]
                 node (create-node brokers)]

             ;;dummy test because we expect an exception
             1 => false
             (node/shutdown-node! node))
           (catch Throwable t
             (do
               (:type (ex-data t)) => :metadata-exception))))

  (facts "Test create node with false broker"
         (let [brokers (conj (get-in resources [:kafka :brokers]) {:host "bla" :port 111})
               node (create-node brokers)]

           (send-test-data brokers 1000)

           (Thread/sleep 1000)
           (node/shutdown-node! node)

           ;dummy check, if no exception is thrown its ok
           1 => 1)))

(let [resources (startup-resources "test")]
  (try
    (test-facts resources)
    (finally
      (shutdown-resources resources))))
