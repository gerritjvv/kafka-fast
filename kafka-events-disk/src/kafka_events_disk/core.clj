(ns kafka-events-disk.core
  "A utility function for kafka-clj that write's events in the work-unit-event-ch
   to local disk using the fileape library"
  (:require [clojure.core.async :as async]
            [fileape.core :as fape]
            [clojure.data.json :as json])
  (:import [java.io DataOutputStream]))


(def writers (ref {}))

(defn- create-ape
  "@params conf keys path can also contain keys for fileape to override any defaults
   @return fape/ape"
  [{:keys [path] :as conf}]
  {:pre [path]}
  (fape/ape
        (merge
          {:codec :gzip
           :base-dir path
           :check-freq 1000
           :rollover-size 10485760
           :rollover-timeout 10000
           :roll-callbacks []}
          conf)))

(defn- write-work-unit! [v {:keys [^DataOutputStream out]}]
  (->> v json/write-str (.writeBytes out))
  (.writeBytes out "\n"))

(defn- start-writer [ape work-unit-event-ch]
  (async/go
    (loop []
      (when-let [v (async/<! work-unit-event-ch)]
        (fape/write ape "kafka-events" (partial write-work-unit! v))
        (recur)))))

(defn register-writer!
  "Starts a background go block that reads from work-unit-event-ch
   and writes data to fileape
   @params node keys work-unit-event-ch
           conf keys path
            "
  [{:keys [work-unit-event-ch]} {:keys [path] :as conf}]
  {:pre [work-unit-event-ch path]}
  (let [ape (create-ape conf)]
    (dosync
      (alter writers assoc work-unit-event-ch ape))
    (start-writer ape work-unit-event-ch)))

(defn close-writer!
  "Close the ape connections
  @params node keys work-unit-event-ch"
  [{:keys [work-unit-event-ch]}]
  {:pre [work-unit-event-ch]}
  (if-let [ape (get @writers work-unit-event-ch)]
    (fape/close ape))
  (dosync (alter writers dissoc work-unit-event-ch)))




