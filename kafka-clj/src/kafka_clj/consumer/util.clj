(ns kafka-clj.consumer.util
  (:require
    [kafka-clj.fetch :refer [create-offset-producer] :as fetch]
    [clojure.core.async :refer [timeout alts!!]]
    [clojure.tools.logging :refer [info error]]
    [kafka-clj.metadata :as meta]
    [kafka-clj.tcp :as tcp])
  (:import (io.netty.buffer Unpooled)))


(defn get-create-offset-producer [offset-producers-ref broker conf]
  (if-let [producer (get @offset-producers-ref broker)]
    producer
    (get
      (dosync
        (alter offset-producers-ref
               (fn [m]
                 (if-let [producer (get m broker)]
                   m
                   (assoc m broker (create-offset-producer broker conf))))))
      broker)))

(defn get-offset-producer!
  "Get a producer, if closed we remove the broker from the offset-producers-ref
   and shutdown the producer associated and return nil,
   this will allow the producer to be blacklisted first, and then
   at a later timeout to recreate the producer, this behaviour avoids
   bursts of close/create sequences which could make the application unstable."
  [offset-producers-ref broker conf]
  (let [producer (get-create-offset-producer offset-producers-ref broker conf)]
    (if (fetch/offset-producer-closed? producer)
      (do                                                   ;check for closed
        (dosync                                             ;if so remove broker and close producer
          (alter offset-producers-ref dissoc broker))
        (fetch/shutdown-offset-producer producer)
        nil)
      producer)))

(defn get-offsets [offset-producer topic partitions]
  "returns [{:topic topic :partitions {:partition :error-code :offsets}}]"
  ;we should send format [[topic [{:partition 0} {:partition 1}...]] ... ]

  (fetch/send-offset-request offset-producer [[topic (map (fn [x] {:partition x}) partitions)]] )
  (fetch/read-offset-response offset-producer))

(defn transform-offsets [topic offsets-response {:keys [use-earliest] :or {use-earliest true}}]
  "Transforms [{:topic topic :partitions {:partition :error-code :offsets}}]
   to {topic [{:offset offset :partition partition}]}"
  (let [topic-data (first (filter #(= (:topic %) topic) offsets-response))
        partitions (:partitions topic-data)]
    {(:topic topic-data)
      (doall (for [{:keys [partition error-code offsets]} partitions]
               {:offset (if use-earliest (last offsets) (first offsets))
                :all-offsets offsets
                :error-code error-code
                :locked false
                :partition partition}))}))

(defn get-broker-offsets [{:keys [offset-producers blacklisted-offsets-producers-ref]} metadata topics conf]
  "Builds the datastructure {broker {topic [{:offset o :partition p} ...] }}"
  (apply merge-with merge
         (for [topic topics]
           (let [topic-data (get metadata topic)
                 by-broker (group-by second (map-indexed vector topic-data))]
             (into {}
                   (for [[broker v] by-broker :when (:host broker)]
                     ;here we have data {{:host "localhost", :port 1} [[0 {:host "localhost", :port 1}] [1 {:host "localhost", :port 1}]], {:host "abc", :port 1} [[2 {:host "abc", :port 1}]]}
                     ;doing map first v gives the partitions for a broker
                     (do
                       (try
                         (let [offset-producer (get-create-offset-producer offset-producers broker conf)

                               ;throw exception if the offset-producer is nil
                               _ (if (not offset-producer) (throw (RuntimeException. (str "No producer found or bad producer connection for " broker))))

                               offsets-response (get-offsets offset-producer topic (map first v))]

                           [broker (transform-offsets topic offsets-response conf)])
                         (catch Exception e (do
                                              (error e e)
                                              (meta/black-list-producer! blacklisted-offsets-producers-ref {:host (:host broker) :port (:port broker)} e)
                                              [broker nil]))))))))))
