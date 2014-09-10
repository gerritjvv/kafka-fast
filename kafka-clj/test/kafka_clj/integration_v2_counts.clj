(ns kafka-clj.integration-v2-counts
  (:require [kafka-clj.util :refer [startup-resources shutdown-resources create-topics]]
            [kafka-clj.client :refer [create-connector send-msg close]]
            [kafka-clj.consumer.node :refer [create-node! read-msg! shutdown-node!]])
  (:use midje.sweet))

;Test that we can produce N messages and consumer N messages.
;:it tag for integration tests

(def state-ref (atom nil))
(def node-ref (atom nil))
(def client-ref (atom nil))

(defn- uniq-name []
  (str (System/currentTimeMillis)))

(defn- send-test-messages [c topic n]
  (dotimes [i n]
    (send-msg c topic (.getBytes (str "my-test-message-" i)))))

(defn- setup-test-data [topic n]
  (send-test-messages @client-ref topic n))

(defonce test-topic (uniq-name))

(with-state-changes
  [ (before :facts (do (reset! state-ref (startup-resources test-topic))
                       (reset! client-ref (create-connector (get-in @state-ref [:kafka :brokers]) {}))
                       (reset! node-ref (create-node!
                                          {:bootstrap-brokers (get-in @state-ref [:kafka :brokers])
                                           :redis-conf {:host "localhost"
                                                        :port (get-in @state-ref [:redis :port])
                                                        :max-active 5 :timeout 1000 :group-name (uniq-name)}
                                           :conf {}}
                                          [test-topic]))
                       (Thread/sleep 1000)
                       (setup-test-data test-topic 100000)))
    (after :facts (do
                    (close @client-ref)
                    (shutdown-node! @node-ref)
                    (shutdown-resources @state-ref)))]

  (fact "Test message counts" :it

        ))