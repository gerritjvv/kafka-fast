(ns
  ^{:doc "Create topics in kafka with auto create


  ----------- CREATE TOPICS ----------

  CreateTopics Request (Version: 0) => [create_topic_requests] timeout
    create_topic_requests => topic num_partitions replication_factor [replica_assignment] [configs]
      topic => STRING
      num_partitions => INT32
      replication_factor => INT16
      replica_assignment => partition_id [replicas]
        partition_id => INT32
        replicas => INT32
      configs => config_key config_value
        config_key => STRING
        config_value => STRING
      timeout => INT32

    CreateTopics Response (Version: 0) => [topic_error_codes]
      topic_error_codes => topic error_code
        topic => STRING
        error_code => INT16

  "}
  kafka-clj.topics
  (:require [kafka-clj.tcp-api :as tcp-api]
            [kafka-clj.buff-utils :as buff-utils]
            [kafka-clj.protocol :as protocol])
  (:import (io.netty.buffer ByteBuf Unpooled)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; private functions

(defn create-topic-request [^ByteBuf buff {:keys [topic partitions replication-factor]} timeout-ms]
  {:pre [topic (number? partitions) (number? replication-factor)]}
  (buff-utils/write-short-string buff topic)
  (.writeInt buff (int partitions))
  (.writeShort buff (int replication-factor))
  (.writeInt buff (int 0))                                  ;empty replica_assignment array
  (.writeInt buff (int 0))                                  ;empty configs array
  (.writeInt buff (int timeout-ms)))


(defn read-create-topics-response
  "return [{:topic <topic> :error_code <kafka-error-code>} ....]]"
  [^ByteBuf buff]
  (let [
        size (.readInt buff)]

    ;;for some reason size is not what it should be here, so we ignore by checking for an exception condition
    (loop [msgs []]
      (let [msg (try
                  {:topic (buff-utils/read-short-string buff)
                   :error_code (.readShort buff)}
                  (catch Exception _ nil))]

        (if msg
          (recur (conj msgs msg))
          (filterv :topic msgs))))))

(defn check-request-package-structure [topic]
  (if (and (string? (:topic topic)) (number? (:partitions topic)) (number? (:replication-factor topic)))
    topic
    (throw (RuntimeException. (str topic " must have :topic <string>, :partitions <num> and :replication-factor <num> keys")))))

(defn create-topic-request-package [topic]
  (if (string? topic)
    {:topic topic :partitions 2 :replication-factor 1}      ;;for some reason the broker for 0.10.1 throws number of partitions must be larger than 0 with -1 here
    (check-request-package-structure topic)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; public functions

(defn create-topics
  "Write a topic create request to the kafka broker client

   response => ( {:topic \"mytopic123\", :error_code 0})

   "
  [client topics timeout-ms]
  {:pre [(:output client)]}
  (when (pos? (count topics))
    (let [create-structures (reduce #(conj %1 (create-topic-request-package %2)) [] topics)

          ^ByteBuf buff (Unpooled/buffer)]

      ;;; mutate buff:ByteBuff by writing requests to it, and
      ;;; updating the first 4 bytes with the final size
      (buff-utils/with-size buff
                            (fn [^ByteBuf buff]
                              (.writeShort buff (short protocol/API_CREATE_TOPICS))
                              (.writeShort buff (short protocol/API_VERSION))
                              (.writeInt buff (int (protocol/unique-corrid!)))
                              (buff-utils/write-short-string buff nil)

                              (.writeInt buff (int (count create-structures)))

                              (doseq [create-structure create-structures]
                                (create-topic-request buff create-structure timeout-ms))))


      (tcp-api/write! client buff :flush true)
      (read-create-topics-response (Unpooled/wrappedBuffer (tcp-api/read-response nil client timeout-ms))))))