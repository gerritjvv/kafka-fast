(ns kafka-clj.consumer.work-organiser
  (:require 
     [clojure.tools.logging :refer [error]]
     [taoensso.carmine :as car :refer [wcar]]
     [group-redis.core :as gr]
     [kafka-clj.metadata :refer [get-metadata]]
     [kafka-clj.produce :refer [metadata-request-producer]]
     [kafka-clj.consumer :refer [get-broker-offsets]]
     [group-redis.core :refer [persistent-get persistent-set]]
     ))

;;; This namespace requires a running redis and kafka cluster
;;;;;;;;;;;;;;;;;; USAGE ;;;;;;;;;;;;;;;
;(use 'kafka-clj.consumer.work-organiser :reload)
;(def org (create-organiser!   
;{:bootstrap-brokers [{:host "localhost" :port 9092}]
; :redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))
; (calculate-new-work org ["ping"])
;
;
;(use 'kafka-clj.consumer.consumer :reload)
;(def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))
;
;(def res (do-work-unit! consumer (fn [state status resp-data] state)))
;

(defn calculate-work-units 
  "Returns '({:topic :partition :offset :len}) Len is exclusive"
  [producer topic partition max-offset start-offset step]
  {:pre [(and (:host producer) (:port producer))]}
  (if (< start-offset max-offset)
    (let [t (+ start-offset step)
          l (if (> t max-offset) (- max-offset start-offset) step)]
      (cons
        {:topic topic :partition partition :offset start-offset :len l}
        (lazy-seq 
          (calculate-work-units producer topic partition max-offset (+ start-offset l) step))))))

(defn- to-int [s]
  (if (integer? s) 
    s 
    (if (> (count s) 0) 
      (Long/parseLong (str s))
      0)))

(defn get-saved-offset 
  "Returns the last saved offset for a topic partition combination"
  [{:keys [group-conn]} topic partition]
  {:pre [group-conn topic partition]}
  (let [offset (to-int (persistent-get group-conn (str "offsets/" topic "/" partition)))]
    (if offset offset 0)))

(defn add-offsets [state topic offset-datum]
  (try
    (assoc offset-datum :saved-offset (get-saved-offset state topic (:partition offset-datum)))
    (catch Throwable t (do (.printStackTrace t) (error t t) nil))))


(defn send-offsets-if-any! 
  ""
  [{:keys [group-conn redis-conn work-queue consume-step] :as state :or {consume-step 1000}} broker topic offset-data]
  {:pre [group-conn redis-conn work-queue consume-step (integer? consume-step)]}
  (let [offset-data2 
        (filter (fn [x] (and x (< (:saved-offset x) (:offset x)))) (map (partial add-offsets state topic) offset-data))]
    (doseq [{:keys [offset partition saved-offset]} offset-data2]
       ;w-units
      ;max offset
      ;push w-units
      ;save max-offset
      (let [work-units (calculate-work-units broker topic partition offset saved-offset consume-step)
            max-offset (apply max (map #(+ (:offset %) (:len %)) work-units))]
        (car/wcar redis-conn
          (apply car/lpush work-queue (map #(assoc % :producer broker) work-units))
          (persistent-set group-conn (str "offsets/" topic "/" partition) max-offset))))))
        
(defn calculate-new-work [{:keys [meta-producers conf] :as state} topics]
  {:pre [meta-producers conf]}
  (let [meta (get-metadata meta-producers conf)
        offsets (get-broker-offsets state meta topics conf)]
        ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
        (doseq [[broker topic-data] offsets]
          (doseq [[topic offset-data] topic-data]
            (try 
              ;we map :offset to max of :offset and :all-offets
              (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
              (catch Exception e (error e e)))))
        state))


(defn get-queue-data [{:keys [redis-conn]} queue-name & {:keys [limit] :or {limit -1}}]
  (car/wcar redis-conn (car/lrange queue-name 0 limit)))

(defn create-meta-producers! 
  "For each boostrap broker a metadata-request-producer is created"
  [bootstrap-brokers conf]
  (doall (map #(metadata-request-producer (:host %) (:port %) conf) bootstrap-brokers)))

(defn create-organiser! 
  "Create a organiser state that should be passed to all the functions were state is required in this namespace"
  [{:keys [bootstrap-brokers work-queue working-queue complete-queue redis-conf conf] :as state}]
  {:pre [work-queue working-queue complete-queue
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port) (:host redis-conf)] }
  
  (let [meta-producers (create-meta-producers! bootstrap-brokers conf)
        group-conn (gr/create-group-connector (:host redis-conf) redis-conf)
        redis-conn redis-conf]
    (assoc state :meta-producers meta-producers :group-conn group-conn :redis-conn redis-conn :offset-producers (ref {}))))
    
    
    
(comment
  
  (use 'kafka-clj.consumer.work-organiser :reload)

(def org (create-organiser!  
 
 {:bootstrap-brokers [{:host "localhost" :port 9092}]
  :redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))


(calculate-new-work org ["ping"])



(use 'kafka-clj.consumer.consumer :reload)
(def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))

(def res (do-work-unit! consumer (fn [state status resp-data] state)))

)
    
    
    
    
  