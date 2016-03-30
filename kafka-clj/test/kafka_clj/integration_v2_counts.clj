(ns kafka-clj.integration-v2-counts
  (:require [kafka-clj.test-utils :refer [startup-resources shutdown-resources]]
            [kafka-clj.consumer.work-organiser :refer [wait-on-work-assigned-flag]]
            [kafka-clj.client :refer [create-connector send-msg close]]
            [kafka-clj.consumer.node :refer [create-node! read-msg! shutdown-node!] :as node]
            [clojure.tools.logging :refer [info error]])
  (:use midje.sweet))

;Test that we can produce N messages and consumer N messages.
;:it tag for integration tests

(def state-ref (atom nil))
(def node-ref (atom nil))
(def client-ref (atom nil))

(defn- uniq-name []
  (str (System/currentTimeMillis)))

(defonce test-range (range 100))

(defn- send-test-messages [c topic n]
  (dotimes [i n]
    (send-msg c topic (.getBytes (str (take (rand-int 100) test-range) i)))))

(defn- setup-test-data [topic n]
  (send-test-messages @client-ref topic n))

(defn- read-messages [node]
  (loop [msgs []]
    (if-let [msg (read-msg! node 10000)]
      (do
        (recur (conj msgs msg)))
      msgs)))

(defonce test-topic (uniq-name))
(defonce msg-count 10000)

(with-state-changes
  [ (before :facts (do (reset! state-ref (startup-resources 1 test-topic))
                       (reset! client-ref (create-connector (get-in @state-ref [:kafka :brokers]) {:flush-on-write true :codec 2}))
                       (reset! node-ref (create-node!
                                          {:bootstrap-brokers (get-in @state-ref [:kafka :brokers])
                                           :redis-conf {:host "localhost"
                                                        :port (get-in @state-ref [:redis :port])
                                                        :max-active 5 :timeout 1000 :group-name (uniq-name)}
                                           :conf {:use-earliest true
                                                  :work-calculate-freq 200}}
                                          [test-topic]))
                       (setup-test-data test-topic msg-count)))
    (after :facts (do
                    (close @client-ref)
                    (shutdown-node! @node-ref)
                    (shutdown-resources @state-ref)))]

  (fact "Test message counts" :it


        ;allows us to wait till the work assignment has started
        (wait-on-work-assigned-flag (:org @node-ref) 30000)
        (prn ">>>>>>>>>>>>>>>>>>> completed wait-on-work-assignment")

        (let [msgs (read-messages @node-ref)]
          (prn "->>>>>>>>>>>>>> got messages " (count msgs))
          (count msgs) => msg-count)


        ;;test monitoring options
        (pos? (node/msg-chan-byte-size @node-ref)) => true
        (pos? (node/conn-pool-byte-size @node-ref)) => true
        (not (neg? (node/conn-pool-active @node-ref))) => true
        (not (neg? (node/conn-pool-idle @node-ref))) => true))