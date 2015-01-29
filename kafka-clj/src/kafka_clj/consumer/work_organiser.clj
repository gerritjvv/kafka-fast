(ns kafka-clj.consumer.work-organiser
  (:require
    [kafka-clj.consumer.util :as cutil]
    [clojure.tools.logging :refer [info debug error]]
    [taoensso.carmine :as car]
    [kafka-clj.redis :as redis]
    [fun-utils.core :as fu]
    [kafka-clj.metadata :refer [get-metadata get-metadata-recreate!]]
    [kafka-clj.produce :refer [metadata-request-producer] :as produce]
    [kafka-clj.consumer.consumer :refer [wait-on-work-unit!]])
  (:import [java.util.concurrent Executors ExecutorService CountDownLatch TimeUnit]
           (java.util.concurrent.atomic AtomicBoolean)))

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

(defn- work-complete-fail-delete!
  "
   Deletes the work unit by doing nothing i.e the work unit will not get pushed to the work queue again"
  [{:keys [redis-conn error-queue] :as state} wu]
  (error "delete workunit error-code:1 " + wu)
  (car/lpush error-queue (into (sorted-map) wu)))

(defn- work-complete-fail!
  "
   Tries to recalcualte the broker
   Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} w-unit]
  (try
    ;we try to recalculate the broker, if any exception we reput the w-unit on the queue
    (let [sorted-wu (into (sorted-map) w-unit)
          broker (get-broker-from-meta state (:topic w-unit) (:partition w-unit))]
      (debug "Rebuilding failed work unit " w-unit)
      (car/lpush work-queue (assoc sorted-wu :producer broker))
      state)
    (catch Exception e (do
                        (error e e)
                        (car/lpush work-queue (into (sorted-map) w-unit))))))

(defn- ensure-unique-id [w-unit]
  ;function for debug purposes
  w-unit)

(defn- work-complete-ok!
  "If the work complete queue w-unit :status is ok
   If [diff = (offset + len) - read-offset] > 0 then the work is republished to the work-queue with offset == (inc read-offset) and len == diff

   Side effects: Send data to redis work-queue"
  [{:keys [work-queue] :as state} {:keys [resp-data offset len] :as w-unit}]
  {:pre [work-queue resp-data offset len]}
  (let [sorted-wu (into (sorted-map) w-unit)
        offset-read (to-int (:offset-read resp-data))]
    (if (< offset-read (to-int offset))
      (do
          (info "Pushing zero read work-unit  " w-unit " sending to recalc")
          (work-complete-fail! state sorted-wu))
      (let [new-offset (inc offset-read)
            diff (- (+ (to-int offset) (to-int len)) new-offset)]
        (if (> diff 0)                                      ;if any offsets left, send work to work-queue with :offset = :offset-read :len diff
          (let [new-work-unit (assoc (dissoc sorted-wu :resp-data) :offset new-offset :len diff)]
            (info "Recalculating work for processed work-unit " new-work-unit " prev-wu " w-unit)
            (car/lpush
              work-queue
              new-work-unit)))))
    state))

(defn- ^Long safe-num
  "Quick util function that returns a positive number or 0"
  [^Long x]
  (if (pos? x) x 0))

(defn- get-queue-len [redis-conn queue]
  (car/wcar redis-conn (car/llen queue)))




(defn work-complete-handler!
  "If the status of the w-unit is :ok the work-unit is checked for remaining work, otherwise its completed, if :fail the work-unit is sent to the work-queue.
   Must be run inside a redis connection e.g car/wcar redis-conn"
  [{:keys [redis-conn] :as state} {:keys [status] :as w-unit}]
  (condp = status
    :fail (work-complete-fail! state w-unit)
    :fail-delete (work-complete-fail-delete! state w-unit)
    (work-complete-ok! state w-unit)))


(defn- ^Runnable work-complete-loop
  "Returns a Function that will loop continueously and wait for work on the complete queue"
  [{:keys [redis-conn complete-queue working-queue ^AtomicBoolean shutdown-flag ^CountDownLatch shutdown-confirm] :as state}]
  {:pre [redis-conn complete-queue working-queue]}
  (fn []
    (try
      (while (and (not (Thread/interrupted)) (not (.get shutdown-flag)))
        (try
          (when-let [work-units (wait-on-work-unit! redis-conn complete-queue working-queue)]
            (redis/wcar
              redis-conn
              (if (map? work-units)
                (do
                  (work-complete-handler! state (ensure-unique-id work-units))
                  (car/lrem working-queue -1 (into (sorted-map) work-units)))
                (doseq [work-unit work-units]
                  (work-complete-handler! state (ensure-unique-id work-unit))
                  (car/lrem working-queue -1 (into (sorted-map) work-unit))))))
          (catch InterruptedException e1 (info "Exit work complete loop"))
          (catch Exception e (do (error e e) (prn e)))))
      (finally
        (.countDown shutdown-confirm)))))

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
        {:topic topic :partition partition :offset start-offset :len l :max-offset (+ start-offset l)}
        (lazy-seq
          (calculate-work-units producer topic partition max-offset (+ start-offset l) step))))))

(defn ^Long get-saved-offset
  "Returns the last saved offset for a topic partition combination
   If the partition is not found for what ever reason the latest offset will be taken from the kafka meta data
   this value is saved to redis and then returned"
  [{:keys [group-name redis-conn] :as state} topic partition]
  {:pre [group-name redis-conn topic partition]}
  (io!
    (let [saved-offset (redis/wcar redis-conn (car/get (str "/" group-name "/offsets/" topic "/" partition)))]
      (cond
        (not-empty saved-offset) (to-int saved-offset)
        :else (let [meta-offset (get-offset-from-meta state topic partition)]
                (info "Set initial offsets [" topic "/" partition "]: " meta-offset)
                (redis/wcar redis-conn (car/set (str "/" group-name "/offsets/" topic "/" partition) meta-offset))
                meta-offset)))))

(defn add-offsets! [state topic offset-datum]
  (try
    (assoc offset-datum :saved-offset (get-saved-offset state topic (:partition offset-datum)))
    (catch Throwable t (do (.printStackTrace t) (error t t) nil))))

(defn send-offsets-if-any!
  "
  Calculates the work-units for saved-offset -> offset and persist to redis.
  Side effects: lpush work-units to work-queue
                set offsets/$topic/$partition = max-offset of work-units
   "
  [{:keys [group-name redis-conn work-queue ^Long consume-step work-assigned-flag conf] :as state} broker topic offset-data]
  {:pre [group-name redis-conn work-queue]}
  (let [offset-data2
        ;here we add offsets as saved-offset via the add-offset function
        ;note that this function will also write to redis
        (filter (fn [x] (and x (< ^Long (:saved-offset x) ^Long (:offset x)))) (map (partial add-offsets! state topic) offset-data))

        consume-step2 (if consume-step consume-step (get conf :consume-step 100000))]

    (doseq [{:keys [offset partition saved-offset]} offset-data2]
      (swap! work-assigned-flag inc)
      ;w-units
      ;max offset
      ;push w-units
      ;save max-offset
      ;producer topic partition max-offset start-offset step
      (if-let [work-units (calculate-work-units broker topic partition offset saved-offset consume-step2)]
        (let [max-offset (apply max (map #(+ ^Long (:offset %) ^Long (:len %)) work-units))
              ts (System/currentTimeMillis)]
          (redis/wcar redis-conn
                      ;we must use sorted-map here otherwise removing the wu will not be possible due to serialization with arbritary order of keys
                    (apply car/lpush work-queue (map #(assoc (into (sorted-map) %) :producer broker :ts ts) work-units))
                    (car/set (str "/" group-name "/offsets/" topic "/" partition) max-offset))
          )))))

(defn get-offset-from-meta [{:keys [meta-producers conf] :as state} topic partition]
  (let [meta (get-metadata meta-producers conf)
        offsets (cutil/get-broker-offsets state meta [topic] conf)
        partition-offsets (->> offsets vals (mapcat #(get % topic)) (filter #(= (:partition %) partition)) first :all-offsets)]

    (if (empty? partition-offsets)
      (throw (ex-info "No offset data found" {:topic topic :partition partition :offsets offsets})))

    (if (= true (:use-earliest conf)) (apply min partition-offsets) (apply max partition-offsets))))

(defn- topic-partition? [m topic partition] (filter (fn [[t partition-data]] (and (= topic t) (not-empty (filter #(= (:partition %) partition) partition-data)) )) m))

(defn get-broker-from-meta [{:keys [meta-producers conf use-earliest] :as state} topic partition & {:keys [retry-count] :or {retry-count 0}}]
  (let [meta (get-metadata meta-producers conf)
        offsets (cutil/get-broker-offsets state meta [topic] conf)
        ;;all this to get the broker ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
        brokers (filter #(not (nil? %)) (map (fn [[broker data]] (if (not-empty (topic-partition? data topic partition)) broker nil)) offsets))]

    (if (empty? brokers)
      (if (> retry-count 2)
        (throw (ex-info "No broker data found" {:topic topic :partition partition :offsets offsets}))
        (get-broker-from-meta state topic partition :retry-count (inc retry-count)))
      (first brokers))))

(defn calculate-new-work
  "Accepts the state and returns the state as is.
   For topics new work is calculated depending on the metadata returned from the producers"
  [{:keys [meta-producers conf error-handler] :as state} topics]
  {:pre [meta-producers conf]}
  (let [[meta-producers1 meta] (get-metadata-recreate! @meta-producers conf)
         offsets (cutil/get-broker-offsets state meta topics conf)]
    ;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}
    (dosync (alter meta-producers (fn [_] meta-producers1)))

    (doseq [[broker topic-data] offsets]

      (doseq [[topic offset-data] topic-data]
        (try
          ;we map :offset to max of :offset and :all-offets
          (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
          (catch Exception e (do (error e e) (.printStackTrace e) (if error-handler (error-handler :meta state e)))))))
    (assoc state :meta-producers meta-producers1)))


(defn get-queue-data
  "Helper function that returns the data from a queue using lrange 0 limit"
  [{:keys [redis-conn]} queue-name & {:keys [limit] :or {limit -1}}]
  (redis/wcar redis-conn (car/lrange queue-name 0 limit)))

(defn put-queue-data
  "Helper function that returns the data from a queue using lrange 0 limit"
  [{:keys [redis-conn]} queue-name data]
  (redis/wcar redis-conn (car/lpush queue-name data)))


(defn remove-queue-data
  "Helper function that returns the data from a queue using lrange 0 limit"
  [{:keys [redis-conn]} queue-name data]
  (redis/wcar redis-conn (car/lrem queue-name -1 data)))



(defn create-meta-producers!
  "For each boostrap broker a metadata-request-producer is created"
  [bootstrap-brokers conf]
  (doall (map #(metadata-request-producer (:host %) (:port %) conf) bootstrap-brokers)))

(defn create-organiser!
  "Create a organiser state that should be passed to all the functions were state is required in this namespace"
  [{:keys [bootstrap-brokers work-queue working-queue complete-queue redis-conf conf] :as state}]
  {:pre [work-queue working-queue complete-queue
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port)]}

  (let [shutdown-flag (AtomicBoolean. false)
        shutdown-confirm (CountDownLatch. 1)
        meta-producers (ref (create-meta-producers! bootstrap-brokers conf))
        spec  {:host     (get redis-conf :host "localhost")
               :port     (get redis-conf :port 6379)
               :password (get redis-conf :password)
               :timeout  (get redis-conf :timeout 4000)}
        opts {:max-active (get redis-conf :max-active 20)}

        redis-conn (redis/conn-pool spec opts)
        intermediate-state (assoc state :meta-producers meta-producers :redis-conn redis-conn :offset-producers (ref {}) :shutdown-flag shutdown-flag :shutdown-confirm shutdown-confirm)
        work-complete-processor-future (start-work-complete-processor! intermediate-state)
        ]
    (assoc intermediate-state
      :work-assigned-flag (atom 0)
      :work-complete-processor-future work-complete-processor-future)))


(defn wait-on-work-assigned-flag [{:keys [work-assigned-flag] :as state} timeout-ms]
  (loop [ts (System/currentTimeMillis)]
    (if (> @work-assigned-flag 0)
      @work-assigned-flag
      (if (>= (- (System/currentTimeMillis) ts) timeout-ms)
        -1
        (do
           (info "waiting on work-assigned-flag: " @work-assigned-flag)
           (Thread/sleep 1000)

            (recur (System/currentTimeMillis)))))))

(defn close-organiser!
  "Closes the organiser passed in"
  [{:keys [meta-producers redis-conn work-complete-processor-future redis-conn ^AtomicBoolean shutdown-flag ^CountDownLatch shutdown-confirm]}]
  {:pre [meta-producers redis-conn (instance? ExecutorService work-complete-processor-future)]}
  (.set shutdown-flag true)
  (.await shutdown-confirm 10000 TimeUnit/MILLISECONDS)
  (.shutdownNow ^ExecutorService work-complete-processor-future)
  (doseq [producer meta-producers]
    (produce/shutdown producer))
  (redis/close-pool redis-conn))


(comment

  (use 'kafka-clj.consumer.work-organiser :reload)

  (def org (create-organiser!
             {:bootstrap-brokers [{:host "localhost" :port 9092}]
              :consume-step      10
              :redis-conf        {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))


  (calculate-new-work org ["test"])



  (use 'kafka-clj.consumer.consumer :reload)
  (def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))

  (def res (do-work-unit! consumer (fn [state status resp-data] state)))

  )




