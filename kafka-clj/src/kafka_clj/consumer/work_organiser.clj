(ns kafka-clj.consumer.work-organiser
  (:import (java.util.concurrent ExecutorService))
  (:require 
     [clojure.tools.logging :refer [error]]
     [taoensso.carmine :as car :refer [wcar]]
     [group-redis.core :as gr]
     [kafka-clj.metadata :refer [get-metadata]]
     [kafka-clj.produce :refer [metadata-request-producer] :as produce]
     [kafka-clj.consumer :refer [get-broker-offsets]]
     [kafka-clj.consumer.consumer :refer [wait-on-work-unit!]]
     [group-redis.core :refer [persistent-get persistent-set]])
  (:import [java.util.concurrent Executors ExecutorService]))

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


(defn- to-int [s]
  (cond
    (integer? s) s
    (empty? s) 0
    :else (Long/parseLong (str s))))

(defn- work-complete-fail!
  "Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} w-unit]
  (car/lpush work-queue w-unit)
  state)

(defn- work-complete-ok!
  "If the work complete queue w-unit :status is ok
   If [diff = (offset + len) - read-offset] > 0 then the work is republished to the work-queue with offset == (inc read-offset) and len == diff

   Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} {:keys [resp-data offset len] :as w-unit}]
  {:pre [work-queue resp-data offset len]}
  (let [diff (- (+ (to-int offset) (to-int len)) (to-int (:offset-read resp-data)))]
    (if (pos? diff)                                         ;if any offsets left, send work to work-queue with :offset = :offset-read :len diff
      (car/lpush
        work-queue
        (assoc (dissoc w-unit :resp-data)  :offset (inc (to-int (:offset-read resp-data)) ) :len diff)
        ))
    state))

(defn work-complete-handler!
  "If the status of the w-unit is :ok the work-unit is checked for remaining work, otherwise its completed, if :fail the work-unit is sent to the work-queue.
   Must be run inside a redis connection e.g car/wcar redis-conn"
  [{:keys [redis-conn] :as state} {:keys [status] :as w-unit}]
  ;(prn "work-complete-handler!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " w-unit)
  (condp = status
    :fail (work-complete-fail! state w-unit)
    (work-complete-ok! state w-unit)))


(defn- ^Runnable work-complete-loop
  "Returns a Function that will loop continueously and wait for work on the complete queue"
  [{:keys [redis-conn complete-queue working-queue] :as state}]
  {:pre [redis-conn complete-queue working-queue]}
  ;@TODO add ATOMIC COUNTER TO COUNT THE NUMBER OF MESSAGES done by the work complete handler
  (fn []
    (while (not (Thread/interrupted))
      (try
        (when-let [work-unit (wait-on-work-unit! redis-conn complete-queue working-queue)]
          (car/wcar redis-conn
                    (work-complete-handler! state work-unit)
                    (car/lrem working-queue -1 work-unit)))
        (catch Exception e (do (error e e) (prn e)))))))

(defn start-work-complete-processor!
  "Creates a ExecutorService and starts the work-complete-loop running in a background thread
   Returns the ExecutorService"
  [state]
  (doto (Executors/newSingleThreadExecutor)
    (.submit (work-complete-loop state))))

(defn calculate-work-units 
  "Returns '({:topic :partition :offset :len}) Len is exclusive"
  [producer topic partition max-offset start-offset step]
  {:pre [(and (:host producer) (:port producer))]}
  (prn "calculate work-units!!!!")
  (if (< start-offset max-offset)
    (let [t (+ start-offset step)
          l (if (> t max-offset) (- max-offset start-offset) step)]
      (cons
        {:topic topic :partition partition :offset start-offset :len l}
        (lazy-seq 
          (calculate-work-units producer topic partition max-offset (+ start-offset l) step))))))

(defn get-saved-offset 
  "Returns the last saved offset for a topic partition combination"
  [{:keys [group-conn]} topic partition]
  {:pre [group-conn topic partition]}
  (io!
    (let [offset (to-int (persistent-get group-conn (str "offsets/" topic "/" partition)))
          ]
      (if offset offset 0))))

(defn add-offsets [state topic offset-datum]
  (try
    (assoc offset-datum :saved-offset (get-saved-offset state topic (:partition offset-datum)))
    (catch Throwable t (do (.printStackTrace t) (error t t) nil))))


(defn send-offsets-if-any! 
  "
  Calculates the work-units for saved-offset -> offset and persist to redis.
  Side effects: lpush work-units to work-queue
                set offsets/$topic/$partition = max-offset of work-units
   "
  [{:keys [group-conn redis-conn work-queue consume-step] :as state :or {consume-step 10}} broker topic offset-data]
  {:pre [group-conn redis-conn work-queue consume-step (integer? consume-step)]}
  (let [offset-data2 
        (filter (fn [x] (and x (< (:saved-offset x) (:offset x)))) (map (partial add-offsets state topic) offset-data))]
    (doseq [{:keys [offset partition saved-offset]} offset-data2]
      ;w-units
      ;max offset
      ;push w-units
      ;save max-offset
      ;producer topic partition max-offset start-offset step
      (let [ work-units (calculate-work-units broker topic partition offset saved-offset consume-step)
            max-offset (apply max (map #(+ (:offset %) (:len %)) work-units))
            ]
        (prn "send-offsets-if-any! >>> push work-units " work-units)
        (car/wcar redis-conn
                  (apply car/lpush work-queue (map #(assoc % :producer broker) work-units)))
        (persistent-set group-conn (str "offsets/" topic "/" partition) max-offset)))))
        
(defn calculate-new-work
  "Accepts the state and returns the state as is.
   For topics new work is calculated depending on the metadata returned from the producers"
  [{:keys [meta-producers conf] :as state} topics]
  {:pre [meta-producers conf]}
  (let [
        meta (get-metadata meta-producers conf)
        offsets (get-broker-offsets state meta topics conf)
        ]
        ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
        (doseq [[broker topic-data] offsets]
          (doseq [[topic offset-data] topic-data]
            (try 
              ;we map :offset to max of :offset and :all-offets
              (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
              (catch Exception e (do (error e e) (.printStackTrace e)) ))))
        state))


(defn get-queue-data
  "Helper function that returns the data from a queue using lrange 0 limit"
  [{:keys [redis-conn]} queue-name & {:keys [limit] :or {limit -1}}]
  (car/wcar redis-conn (car/lrange queue-name 0 limit)))

(defn create-meta-producers! 
  "For each boostrap broker a metadata-request-producer is created"
  [bootstrap-brokers conf]
  (doall (map #(metadata-request-producer (:host %) (:port %) conf) bootstrap-brokers)))

(defn create-organiser!
  "Create a organiser state that should be passed to all the functions were state is required in this namespace"
  [{:keys [bootstrap-brokers work-queue working-queue complete-queue redis-conf conf] :as state}]
  {:pre [work-queue working-queue complete-queue
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port)] }

  (let [meta-producers (create-meta-producers! bootstrap-brokers conf)
        group-conn (gr/create-group-connector (:host redis-conf) redis-conf)
        redis-conn {:pool {:max-active (get redis-conf :max-active 20)}
                    :spec {:host  (get redis-conf :host "localhost")
                           :port    (get redis-conf :port 6379)
                           :password (get redis-conf :password)
                           :timeout  (get redis-conf :timeout 4000)}}
        intermediate-state (assoc state :meta-producers meta-producers :group-conn group-conn :redis-conn redis-conn :offset-producers (ref {}))
        work-complete-processor-future  (start-work-complete-processor! intermediate-state)
        ]
    (prn "create-organiser! created")
    (assoc intermediate-state :work-complete-processor-future work-complete-processor-future)))


(defn close-organiser!
  "Closes the organiser passed in"
  [{:keys [group-conn meta-producers redis-conn work-complete-processor-future]}]
  {:pre [group-conn meta-producers redis-conn (instance? ExecutorService work-complete-processor-future)]}
  (.shutdownNow ^ExecutorService work-complete-processor-future)
  (gr/close group-conn)
  (doseq [producer meta-producers]
    (produce/shutdown producer)))

;TODO create a background thread that will fight to be master then create an organiser on start and close on fail and shutdown

;TODO test organiser
;TODO test work compelte processor

;TODO create timeout processor
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




