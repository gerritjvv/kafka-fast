(ns kafka-clj.offset-utils
  (:require 
    [kafka-clj.metadata :refer [get-metadata]]
    [kafka-clj.produce :refer [metadata-request-producer]]
    [group-redis.core :refer [create-group-connector close persistent-get]]
    [clojure.core.reducers :as r]
    ))

;utility functions to calculate lag and monitor kafka topic consumption

(comment
 
(require '[kafka-clj.metadata :refer [get-metadata]])
(require '[kafka-clj.produce :refer [metadata-request-producer]])
(require '[group-redis.core :refer [create-group-connector close persistent-get]])
            

(def metadata-producer (metadata-request-producer "hb02" 9092 {}))

(def meta (get-metadata [metadata-producer] {}))

;{"test123" [{:host "gvanvuuren-compile", :port 9092} {:host "gvanvuuren-compile", :port 9092}]


(def offsets (get-broker-offsets {:offset-producers (ref {})} meta ["test"] {:use-earliest false}))

{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}


(def g (create-group-connector "localhost"))

)

(defn get-offset-pointer [group-connector topic partition]
   (persistent-get group-connector (str topic "/" partition)))
 

(defn get-topic-offset-pointers [group-connector topic partitions]
   (r/fold (fn 
             ([] [])
             ([state {:keys [offset partition]}] (conj state {:offset offset :partition partition :offset-pointer (get-offset-pointer group-connector topic partition)}))) partitions))



(defn calculate-offset-pointers
  "Returns the offset"
  [group-connector offsets]
  (r/fold (fn 
             ([] {})
             ([broker-state [broker topics]]
              (assoc broker-state 
                 broker (r/fold (fn
                                  ([] {})
                                  ([topic-state [topic partitions]]
                                       (assoc topic-state topic (get-topic-offset-pointers group-connector topic partitions)))) (vec topics))))) (vec offsets)))

