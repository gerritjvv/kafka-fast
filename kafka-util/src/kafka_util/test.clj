(ns kafka-util.test
  (:import (java.util.concurrent TimeUnit))
  (:require [kafka-clj.client :as kprod]
            [kafka-clj.consumer.node :as kc])
  (:import [java.io BufferedWriter]
           [java.util.concurrent Executors ExecutorService TimeUnit]))

"
Run a test to check that the kafka cluster and more importantly the current implementation of the kafka-clj client is
working correctly
"

(defn- write-line
  "Writes the msg as a string and a new line"
  [^BufferedWriter writer msg]
  (doto writer
    (.write (str msg))
    .newLine))

(defn- flush-writer [^BufferedWriter writer] (.flush writer))

(defn- background-flusher [^BufferedWriter writer]
  (let [^ExecutorService service (Executors/newScheduledThreadPool (int 1))
        ^Runnable
        flusher (fn []
                  (try
                    (do
                      (.flush writer)
                      (println "Flushed"))
                    (catch Exception e (.printStackTrace e))))]
    (.scheduleAtFixedRate service flusher 1000 1000 TimeUnit/MILLISECONDS)
    service))

(defn produce-test-messages!
  "Write n messages to the kafka topic each with the format prefix-$i where i = 0 < n"
  [topic prefix n {:keys [bootstrap-brokers]}]
  {:pre [topic prefix (number? n) (coll? bootstrap-brokers)]}
  (io!
    (let [c (kprod/create-connector bootstrap-brokers {})]
      (dotimes [i n]
        (kprod/send-msg c topic (.getBytes (str prefix "-" i))))
      (kprod/close c))))


(defn- consume-messages [msg-seq writer]
  (loop [[msg & rest] msg-seq i 0]
    (when (zero? (mod i 100))
      (prn "Reading messages at index " i))

    (write-line writer (String. ^"[B" (:bts msg)))
    (recur rest (inc i))))


(defn consume-test-messages!
  "Read all messages from a topic, this is done by creating a unique group-name for the consumer
   All messages are printed to a local file
   Note that all msg bytes are converted to a String before written, and all records are newline separated"
  [topic file-out {:keys [bootstrap-brokers redis-conf] :as conf}]
  {:pre [topic file-out (coll? bootstrap-brokers) (map? redis-conf)]}
  (let [ node (kc/create-node! conf [topic])]
    (with-open [writer (clojure.java.io/writer file-out)]
      (let [^ExecutorService service (background-flusher writer)]
        (try
          (do
            (prn "Writing to file " file-out)
            (prn "Using conf " conf)
            (consume-messages (kc/msg-seq! node) writer))
          (finally
            (do
              (println "Close flusher")
              (.flush writer)
              (.shutdownNow service))))))))

(comment
  (consume-test-messages! "utiltest" "/tmp/test.txt" {:bootstrap-brokers [{:host "localhost" :port 9092}] :redis-conf {:host "localhost" :group-name "test"} :conf {}}))