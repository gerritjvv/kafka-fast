(ns kafka-clj.protocol
  (:import (java.util.concurrent.atomic AtomicInteger)))

(defonce ^:constant API_KEY_PRODUCE_REQUEST (short 0))
(defonce ^:constant API_KEY_FETCH_REQUEST (short 1))
(defonce ^:constant API_KEY_OFFSET_REQUEST (short 2))
(defonce ^:constant API_KEY_METADATA_REQUEST (short 3))
(defonce ^:constant API_KEY_SASL_HANDSHAKE (short 17))


(defonce ^:constant API_VERSION (short 0))

(defonce ^:constant MAGIC_BYTE (int 0))

(defonce ^AtomicInteger corr-counter (AtomicInteger.))

(defn ^Long unique-corrid! []
  (let [v (.getAndIncrement corr-counter)]
    (if (= v Integer/MAX_VALUE)
      (do
        (.set corr-counter 0)
        v)
      v)))