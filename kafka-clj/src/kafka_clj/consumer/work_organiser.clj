(ns kafka-clj.consumer.work-organiser
  (:import (java.util.concurrent ExecutorService))
  (:require
    [kafka-clj.consumer.util :as cutil]
    [clojure.tools.logging :refer [info debug error]]
    [taoensso.carmine :as car :refer [wcar]]
    [group-redis.core :as gr]
    [fun-utils.core :as fu]
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

(declare get-offset-from-meta)
(declare get-broker-from-meta)

(defn- ^Long _to-int [s]
  (try
    (cond
      (integer? s) s
      (empty? s) -10
      :else (Long/parseLong (str s)))
    (catch NumberFormatException n -10)))

(defn- ^Long to-int [s]
  (let [i (_to-int s)]
    (if (= i -10)
      (throw (RuntimeException. (str "Error reading number from s: " s)))
      i)))

(defn- work-complete-fail!
  "
   Tries to recalcualte the broker
   Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} w-unit]
  (try
    ;we try to recalculate the broker, if any exception we reput the w-unit on the queue
    (let [broker (get-broker-from-meta state (:topic w-unit) (:partition w-unit))]
      (error "Rebuilding failed work unit " w-unit)
      (car/lpush work-queue (assoc w-unit :producer broker))
      state)
    (catch Exception e (do
                        (error e e)
                        (car/lpush work-queue w-unit)))))

(defn- ensure-unique-id [w-unit]
  ;function for debug purposes
  w-unit)

(defn- work-complete-ok!
  "If the work complete queue w-unit :status is ok
   If [diff = (offset + len) - read-offset] > 0 then the work is republished to the work-queue with offset == (inc read-offset) and len == diff

   Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} {:keys [resp-data offset len] :as w-unit}]
  {:pre [work-queue resp-data offset len]}
  (let [offset-read (to-int (:offset-read resp-data))]
    (if (< offset-read (to-int offset))
      (do (car/lpush work-queue w-unit)
          (info "Pushing zero read work-unit " w-unit))
      (let [new-offset (inc offset-read)
            diff (- (+ (to-int offset) (to-int len)) new-offset)]
        (if (> diff 0)                                      ;if any offsets left, send work to work-queue with :offset = :offset-read :len diff
          (let [new-work-unit (assoc (dissoc w-unit :resp-data) :offset new-offset :len diff)]
            (info "Recalculating work for processed work-unit " new-work-unit " prev-offset " offset)
            (car/lpush
              work-queue
              new-work-unit)))))
    state))

(defn- ^Long safe-num
  "Quick util function that returns a positive number or 0"
  [^Long x]
  (if (pos? x) x 0))

(defn- do-work-timeout-check!
  "Only if the w-unit has a ts key and the current-ts - ts is greater than work-unit-timeout-ms
   and is seen twice is the workunit removed from the working-queue and placed on the work-queue
   params: state work-unit work-unit-timeout-ms

   Side effects:
     on condition: remove w-unit from working redis queue and push to work redis queue"
  [{:keys [redis-conn working-queue work-queue] :as state} {:keys [ts tm-count] :as w-unit} work-unit-timeout-ms]
  (when (and ts (>= (Math/abs (long (- (System/currentTimeMillis) (to-int ts)))) work-unit-timeout-ms))
    (info "Moving timed out work unit " w-unit " to work-queue " work-queue)
    (if (and tm-count (> tm-count 0))
      (car/wcar redis-conn                  ;we remove the work-unit and push to the work queue
        (car/lrem working-queue -1 w-unit)
        (car/lpush work-queue w-unit))
      (car/wcar redis-conn                  ;we remove the work-unit and push to the working-queue with :tm-count 1
        (car/lrem working-queue -1 w-unit)
        (car/lpush working-queue -1 (assoc w-unit :tm-count 1 :ts (System/currentTimeMillis)))))))

(defn- get-queue-len [redis-conn queue]
  (car/wcar redis-conn (car/llen queue)))

(defn work-timeout-handler!
  "Query the working queue for the last 100 elements and check each for a possible timeout,
   if the work unit has timed out its removed from the working queue and placed on the work-queue"
  [{:keys [redis-conn working-queue] :as state}]
  (try
    (let [work-unit-timeout-ms (* 1000 60 4)
          n 100
          queue-len (to-int (get-queue-len redis-conn working-queue))
          w-units (car/wcar redis-conn
                            (let [^Long x (safe-num (- queue-len n))
                                  ^Long y (+ x n -1)]
                              (car/lrange working-queue
                                          x
                                          y)))]
      (doseq [w-unit w-units]
        (do-work-timeout-check! state w-unit work-unit-timeout-ms)))
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)))))


(defn work-complete-handler!
  "If the status of the w-unit is :ok the work-unit is checked for remaining work, otherwise its completed, if :fail the work-unit is sent to the work-queue.
   Must be run inside a redis connection e.g car/wcar redis-conn"
  [{:keys [redis-conn] :as state} {:keys [status] :as w-unit}]
  ;(prn "work-complete-handler!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " w-unit)
  (condp = status
    :fail (work-complete-fail! state w-unit)
    (work-complete-ok! state w-unit)))

(defn start-work-timeout-processor!
  "Returns a fixed delay that will run the work-timeout-handler every work-unit-timeout-check-freq-ms"
  [{:keys [work-unit-timeout-check-freq-ms] :or {work-unit-timeout-check-freq-ms 10000} :as state}]
  (fu/fixdelay work-unit-timeout-check-freq-ms
               (try
                 (work-timeout-handler! state)
                 (catch Exception e (error e e)))))

(defn- ^Runnable work-complete-loop
  "Returns a Function that will loop continueously and wait for work on the complete queue"
  [{:keys [redis-conn complete-queue working-queue] :as state}]
  {:pre [redis-conn complete-queue working-queue]}
  (fn []
    (while (not (Thread/interrupted))
      (try
        (when-let [work-unit (wait-on-work-unit! redis-conn complete-queue working-queue)]
          (car/wcar redis-conn
                    (work-complete-handler! state (ensure-unique-id work-unit))
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
  [producer topic partition ^Long max-offset ^Long start-offset ^Long step]
  {:pre [(and (:host producer) (:port producer))]}
  (if (< start-offset max-offset)
    (let [^Long t (+ start-offset step)
          ^Long l (if (> t max-offset) (- max-offset start-offset) step)]
      (cons
        {:topic topic :partition partition :offset start-offset :len l}
        (lazy-seq
          (calculate-work-units producer topic partition max-offset (+ start-offset l) step))))))

(defn ^Long get-saved-offset
  "Returns the last saved offset for a topic partition combination
   If the partition is not found for what ever reason the latest offset will be taken from the kafka meta data
   this value is saved to redis and then returned"
  [{:keys [group-conn] :as state} topic partition]
  {:pre [group-conn topic partition]}
  (io!
    (let [saved-offset (persistent-get group-conn (str "offsets/" topic "/" partition))]
      (cond
        (not-empty saved-offset) (to-int saved-offset)
        :else (let [meta-offset (get-offset-from-meta state topic partition)]
                (persistent-set group-conn (str "offsets/" topic "/" partition) meta-offset)
                meta-offset)))))

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
  [{:keys [group-conn redis-conn work-queue ^Long consume-step] :as state :or {consume-step 100000}} broker topic offset-data]
  {:pre [group-conn redis-conn work-queue consume-step (integer? consume-step)]}
  (let [offset-data2
        (filter (fn [x] (and x (< ^Long (:saved-offset x) ^Long (:offset x)))) (map (partial add-offsets state topic) offset-data))]

    (doseq [{:keys [offset partition saved-offset]} offset-data2]
      ;w-units
      ;max offset
      ;push w-units
      ;save max-offset
      ;producer topic partition max-offset start-offset step
      (if-let [work-units (calculate-work-units broker topic partition offset saved-offset consume-step)]
        (let [max-offset (apply max (map #(+ ^Long (:offset %) ^Long (:len %)) work-units))
              ts (System/currentTimeMillis)]
          (car/wcar redis-conn
                    (apply car/lpush work-queue (map #(assoc % :producer broker :ts ts) work-units)))
          (persistent-set group-conn (str "offsets/" topic "/" partition) max-offset))))))

(defn get-offset-from-meta [{:keys [meta-producers conf use-earliest] :as state} topic partition]
  (let [meta (get-metadata meta-producers conf)
        offsets (get-broker-offsets state meta [topic] conf)
        partition-offsets (->> offsets vals (mapcat #(get % topic)) (filter #(= (:partition %) partition)) first :all-offsets)]

    (if (empty? partition-offsets)
      (throw (ex-info "No offset data found" {:topic topic :partition partition :offsets offsets})))

    (if (= true use-earliest) (apply min partition-offsets) (apply max partition-offsets))))

(defn- topic-partition? [m topic partition] (filter (fn [[t partition-data]] (and (= topic t) (not-empty (filter #(= (:partition %) partition) partition-data)) )) m))

(defn get-broker-from-meta [{:keys [meta-producers conf use-earliest] :as state} topic partition]
  (let [meta (get-metadata meta-producers conf)
        offsets (get-broker-offsets state meta [topic] conf)
        ;;all this to get the broker ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
        brokers (filter #(not (nil? %)) (map (fn [[broker data]] (if (not-empty (topic-partition? data topic partition)) broker nil)) offsets))]

    (if (empty? brokers)
      (throw (ex-info "No broker data found" {:topic topic :partition partition})))

    (first brokers)))

(defn calculate-new-work
  "Accepts the state and returns the state as is.
   For topics new work is calculated depending on the metadata returned from the producers"
  [{:keys [meta-producers conf] :as state} topics]
  {:pre [meta-producers conf]}
  (let [meta (get-metadata meta-producers conf)
         offsets (get-broker-offsets state meta topics conf)]
    ;(prn "Offsets " offsets)
    ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
    (doseq [[broker topic-data] offsets]

      (doseq [[topic offset-data] topic-data]
        (try
          ;we map :offset to max of :offset and :all-offets
          (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
          (catch Exception e (do (error e e) (.printStackTrace e))))))
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
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port)]}

  (let [meta-producers (create-meta-producers! bootstrap-brokers conf)
        group-conn (gr/create-group-connector (:host redis-conf) redis-conf)
        redis-conn {:pool {:max-active (get redis-conf :max-active 20)}
                    :spec {:host     (get redis-conf :host "localhost")
                           :port     (get redis-conf :port 6379)
                           :password (get redis-conf :password)
                           :timeout  (get redis-conf :timeout 4000)}}
        intermediate-state (assoc state :meta-producers meta-producers :group-conn group-conn :redis-conn redis-conn :offset-producers (ref {}))
        work-complete-processor-future (start-work-complete-processor! intermediate-state)
        work-timeout-processor-fdelay (start-work-timeout-processor! intermediate-state)
        ]
    (assoc intermediate-state :work-complete-processor-future work-complete-processor-future
                              :work-timeout-processor-fdelay work-timeout-processor-fdelay)))


(defn close-organiser!
  "Closes the organiser passed in"
  [{:keys [group-conn meta-producers redis-conn work-complete-processor-future work-timeout-processor-fdelay]}]
  {:pre [group-conn meta-producers redis-conn (instance? ExecutorService work-complete-processor-future) work-timeout-processor-fdelay]}
  (.shutdownNow ^ExecutorService work-complete-processor-future)
  (gr/close group-conn)
  (doseq [producer meta-producers]
    (produce/shutdown producer))

  (if work-timeout-processor-fdelay
    (fu/stop-fixdelay work-timeout-processor-fdelay))
  )


;TODO TEST timeout processor

(comment

  (use 'kafka-clj.consumer.work-organiser :reload)

  (def org (create-organiser!
             {:bootstrap-brokers [{:host "hb02" :port 9092}]
              :consume-step      10
              :redis-conf        {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))


  (calculate-new-work org ["adx-bid-requests"])



  (use 'kafka-clj.consumer.consumer :reload)
  (def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))

  (def res (do-work-unit! consumer (fn [state status resp-data] state)))

  )




