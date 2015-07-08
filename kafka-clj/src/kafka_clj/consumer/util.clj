(ns kafka-clj.consumer.util
  (:require
    [kafka-clj.fetch :refer [create-offset-producer send-offset-request]]
    [clojure.core.async :refer [timeout alts!!]]
    [clojure.tools.logging :refer [info error]]
    ))


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

(defn get-offsets [offset-producer topic partitions]
  "returns [{:topic topic :partitions {:partition :error-code :offsets}}]"
  ;we should send format [[topic [{:partition 0} {:partition 1}...]] ... ]

  (send-offset-request offset-producer [[topic (map (fn [x] {:partition x}) partitions)]] )

  (let [{:keys [offset-timeout] :or {offset-timeout 10000}} (:conf offset-producer)
        {:keys [read-ch error-ch]} (:client offset-producer)
        [v c] (alts!! [read-ch error-ch (timeout offset-timeout)])
        ]

    (if v
      (if (= c read-ch)
        v
        (throw (RuntimeException. (str "Error reading offsets from " offset-producer " for topic " topic " error: " v))))
      (throw (RuntimeException. (str "Timeout while reading offsets from " offset-producer " for topic " topic))))))

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

(defn get-broker-offsets [{:keys [offset-producers]} metadata topics conf]
  "Builds the datastructure {broker {topic [{:offset o :partition p} ...] }}"
  (apply merge-with merge
         (for [topic topics]
           (let [topic-data (get metadata topic)
                 by-broker (group-by second (map-indexed vector topic-data))]
             (into {}
                   (for [[broker v] by-broker]
                     ;here we have data {{:host "localhost", :port 1} [[0 {:host "localhost", :port 1}] [1 {:host "localhost", :port 1}]], {:host "abc", :port 1} [[2 {:host "abc", :port 1}]]}
                     ;doing map first v gives the partitions for a broker
                     (try
                       (let [offset-producer (get-create-offset-producer offset-producers broker conf)
                             offsets-response (get-offsets offset-producer topic (map first v))]
                         ;(info "offsets: " v)
                         [broker (transform-offsets topic offsets-response conf)])
                       (catch Exception e (do
                                            (error e e)
                                            [broker nil])))))))))
