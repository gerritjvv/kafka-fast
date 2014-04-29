(ns kafka-mon.offsets
  (:require 
    [kafka-clj.metadata :refer [get-metadata]]
    [kafka-clj.produce :refer [metadata-request-producer shutdown]]
    [group-redis.core :refer [create-group-connector close persistent-get]]
    [kafka-clj.offset-utils :refer [calculate-offset-pointers]]
    [kafka-clj.consumer :refer [get-broker-offsets]]))
    
  
(defn parse-offsetpointer [offset-pointer]
  (if offset-pointer 
    (cond 
      (string? offset-pointer) (Long/parseLong offset-pointer)
      :else offset-pointer) 0))

(defn prn-lines [pointers sep]
  "pointers must have format: {{:host gvanvuuren-compile, :port 9092} {test ({:offset 7, :partition 1, :offset-pointer 7} ..."
  (doseq [[{:keys [host port]} broker-topics] pointers]
    (doseq [[topic partitions] broker-topics]
      (doseq [{:keys [offset partition offset-pointer]} partitions]
       (println (clojure.string/join sep [(str host ":" port) topic partition offset (parse-offsetpointer offset-pointer) (- (parse-offsetpointer offset) (parse-offsetpointer offset-pointer))]))))))
  
  
(defn prn-offsets 
  "Prints to STDOUT the broker offsets and consume offsets"
  [group redis-host broker-list format topics]
  (let [conf {}
        metadata-producers (for [{:keys [host port]} broker-list]
	                             (metadata-request-producer host port {}))
	      meta (get-metadata metadata-producers conf)
       _ (do (prn "Meta " meta))
	      offsets (get-broker-offsets {:offset-producers (ref {})} meta topics {:use-earliest false})
       _ (do (prn "Offsets " offsets))
	      g (create-group-connector redis-host)]
     (prn "DONE")
     (try
       (let [pointers (calculate-offset-pointers g offsets)]
         (cond 
           (= format "json")
           (clojure.pprint/pprint pointers)
           (= format "csv")
           (prn-lines pointers ",")
           (= format "tsv")
           (prn-lines pointers "\t")
           :else (throw (Exception. (str "Format " format " is not recougnised")))))
       (finally (close g)))))