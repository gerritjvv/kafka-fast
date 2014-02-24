(ns kafka-ui.models.kafka
  (:require [kafka-clj.metadata :refer [get-metadata]]
            [kafka-clj.consumer :refer [get-broker-offsets]]
            [kafka-ui.models.conf :refer [load-config]]
            [taoensso.carmine :as car :refer [wcar]]
            [group-redis.core :refer [create-group-connector get-members persistent-get empheral-get] :as redis]))

;;return a show kafka related data

(declare get-topic-offsets)


(def conf (load-config))

;TODO get host from db
(def redis-conn (delay (do
                         (prn "using redis server " (:redis conf))
                         (create-group-connector (:redis conf)))))


(defn get-locks [offsets group topic]
   (for [k (sort (car/wcar (:conn @redis-conn)
                    (car/keys (str "/" group "/locks/" topic "/*"))))]
      [k (car/wcar (:conn @redis-conn) (car/get k))]))


(defn get-consumer-members [group]
  (get-members (assoc @redis-conn :group-name group)))

(defn metadata []
  ;TODO get the broker from configuration
  ;Returns {topic [{host port} ...] ...}
  (get-metadata (:kafka-brokers conf) {}))

(defn get-offsets
  ;;Gets the offsets and Returns
  ;;{{:host "gvanvuuren-compile", :port 9092} {"test123"
  ;;               ({:offset 0, :error-code 0, :locked false, :partition 0} {:offset 0, :error-code 0, :locked false, :partition 1})}}
  ([]
   (prn "Use topics " (:topics conf))
   (get-offsets (metadata) (:topics conf)))
  ([meta-data topics]
   (get-broker-offsets meta-data topics {})))


(defn get-groups []
  ;TODO return groups from db
  ["default-group"])

(defn to-int [s]
  (try
    (Integer/parseInt s)
    (catch Exception e 0)))

(defn get-saved-topic-offsets [offsets group topic]
  "Returns {partition offset ... }"
  (into {}
        (for [offset-data (get-topic-offsets offsets topic)]
          [(:partition offset-data)
           (to-int (persistent-get (assoc @redis-conn :group-name group) (str "persistent/" topic "/" (:partition offset-data))))])))

;; helper functions

(defn get-brokers [offsets topic]
  ;;gets the brokers for a topic
  (into (sorted-set) (map (fn [broker topic-map] broker)
                          (filter (fn [broker topic-map]
                               (get topic-map topic)) offsets) offsets)))


(defn get-topics [offsets]
  ;;gets the topics from the topic-map
  (apply sorted-set
         (for [[_ data] offsets
           [topic _] data] topic)))



(defn get-topic-offsets [offsets f-topic]
  (flatten
    (for [[_ topic-map] offsets
          [topic topic-data] topic-map
          :when (= topic f-topic)]
     topic-data)))

(defn offset-total [offsets topic]
  (apply + (map :offset (get-topic-offsets offsets topic))))


