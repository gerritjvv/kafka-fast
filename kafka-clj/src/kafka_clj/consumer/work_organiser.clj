(ns kafka-clj.consumer.work-organiser
  (:require
    [kafka-clj.consumer.util :as cutil]
    [clojure.tools.logging :refer [info debug error]]
    [kafka-clj.redis.core :as redis]
    [kafka-clj.metadata :as meta]
    [fun-utils.cache :as cache]
    [kafka-clj.produce :refer [metadata-request-producer] :as produce]
    [kafka-clj.consumer.workunits :refer [wait-on-work-unit!]])
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
;; Basic data type is the work-unit
;; structure is {:topic topic :partition partition :offset start-offset :len l :max-offset (+ start-offset l) :producer {:host host :port port}}
;;

(declare get-offset-from-meta)
(declare get-broker-from-meta)
(declare close-organiser!)

(defn- ^Long _to-int [s]
  (try
    (cond
      (integer? s) s
      (empty? s) -10
      :else (Long/parseLong (str s)))
    (catch NumberFormatException _ -10)))

(defn- ^Long to-int [s]
  (let [i (_to-int s)]
    (if (= i -10)
      (throw (RuntimeException. (str "Error reading number from s: " s)))
      i)))

(defn- work-complete-fail-delete!
  "
   Deletes the work unit by doing nothing i.e the work unit will not get pushed to the work queue again"
  [{:keys [redis-conn error-queue]} wu]
  (error "delete workunit error-code:1 " + wu)
  (redis/lpush redis-conn error-queue (into (sorted-map) wu)))

(defn- work-complete-fail!
  "
   Tries to recalcualte the broker
   Side effects: Send data to redis work-queue"
  [{:keys [work-queue redis-conn] :as state} w-unit]
  (try
    ;we try to recalculate the broker, if any exception we reput the w-unit on the queue
    (let [sorted-wu (into (sorted-map) w-unit)
          broker (get-broker-from-meta state (:topic w-unit) (:partition w-unit))]
      (debug "Rebuilding failed work unit " w-unit)
      (redis/lpush redis-conn work-queue (assoc sorted-wu :producer broker))
      state)
    (catch Exception e (do
                         (error e e)
                         (redis/lpush redis-conn work-queue (into (sorted-map) w-unit))))))

(defn- ensure-unique-id [w-unit]
  ;function for debug purposes
  w-unit)

(defn- work-complete-ok!
  "If the work complete queue w-unit :status is ok
   If [diff = (offset + len) - read-offset] > 0 then the work is republished to the work-queue with offset == (inc read-offset) and len == diff

   Side effects: Send data to redis work-queue"
  [{:keys [work-queue redis-conn] :as state} {:keys [resp-data offset len] :as w-unit}]
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
            (redis/lpush
              redis-conn
              work-queue
              new-work-unit)))))
    state))

(defn work-complete-handler!
  "If the status of the w-unit is :ok the work-unit is checked for remaining work, otherwise its completed, if :fail the work-unit is sent to the work-queue.
   Must be run inside a redis connection e.g car/wcar redis-conn"
  [state {:keys [status] :as w-unit}]
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
          (when-let [work-units (wait-on-work-unit! redis-conn complete-queue working-queue shutdown-flag)]
            (redis/wcar
              redis-conn
              (if (map? work-units)
                (do
                  (work-complete-handler! state work-units)
                  (redis/lrem redis-conn working-queue -1 (into (sorted-map) work-units)))
                (doseq [work-unit work-units]
                  (work-complete-handler! state work-unit)
                  (redis/lrem redis-conn working-queue -1 (into (sorted-map) work-unit))))))
          (catch InterruptedException _ (info "Exit work complete loop"))
          (catch Exception e (do (error e e) (.printStackTrace e)))))
      (finally
        (.countDown shutdown-confirm)))))

(defn start-work-complete-processor!
  "Creates a ExecutorService and starts the work-complete-loop running in a background thread
   Returns the ExecutorService"
  [state]
  (doto (Executors/newSingleThreadExecutor)
    (.submit (work-complete-loop state))))

(defn start-metadata-unblacklist-processor!
  "Creates a ExecutorService and starts the unblacklist metada producerrunning in a background thread
   Returns the ExecutorService"
  [metadata-producers-ref blacklisted-metadata-producers-ref conf]
  (doto
    (Executors/newSingleThreadScheduledExecutor)
    (.scheduleWithFixedDelay
      (fn []
        (try
          (meta/try-unblacklist! metadata-producers-ref blacklisted-metadata-producers-ref conf)
          (catch InterruptedException _ nil)
          (catch Exception e (error e e))))
      10000
      10000
      TimeUnit/MILLISECONDS)))

(defn calculate-work-units
  "Returns '({:topic :partition :offset :len :max-offset}) Len is exclusive"
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
    (let [saved-offset (redis/wcar redis-conn (redis/get redis-conn (str "/" group-name "/offsets/" topic "/" partition)))]
      (cond
        (not (nil? saved-offset)) (to-int saved-offset)
        :else (let [meta-offset (get-offset-from-meta state topic partition)]
                (info "Set initial offsets [" topic "/" partition "]: " meta-offset)
                (redis/wcar redis-conn (redis/set redis-conn (str "/" group-name "/offsets/" topic "/" partition) meta-offset))
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
        (filter (fn [x] (and x (:saved-offset x) (:offset x) (< ^Long (:saved-offset x) ^Long (:offset x))))
                (map (partial add-offsets! state topic) offset-data))

        consume-step2 (if consume-step consume-step (get conf :consume-step 100000))]

    (doseq [{:keys [offset partition saved-offset]} offset-data2]
      (swap! work-assigned-flag inc)
      ;w-units
      ;max offset
      ;push w-units
      ;save max-offset
      ;producer topic partition max-offset start-offset step
      (when-let [work-units (calculate-work-units broker topic partition offset saved-offset consume-step2)]
        (let [max-offset (apply max (map #(+ ^Long (:offset %) ^Long (:len %)) work-units))
              ts (System/currentTimeMillis)]
          (redis/wcar redis-conn
                      ;we must use sorted-map here otherwise removing the wu will not be possible due to serialization with arbritary order of keys
                      (redis/lpush* redis-conn work-queue (map #(assoc (into (sorted-map) %) :producer broker :ts ts) work-units))
                      (redis/set redis-conn (str "/" group-name "/offsets/" topic "/" partition) max-offset))
          )))))

(defn get-offset-from-meta [{:keys [conf] :as state} topic partition]
  (let [meta (meta/get-metadata! state conf)
        offsets (cutil/get-broker-offsets state meta [topic] conf)
        partition-offsets (->> offsets vals (mapcat #(get % topic)) (filter #(= (:partition %) partition)) first :all-offsets)]

    (if (empty? partition-offsets)
      (throw (ex-info "No offset data found" {:topic topic :partition partition :offsets offsets})))

    (debug "get-offset-from-meta: conf: " (keys conf) " use-earliest: " (:use-earliest conf) " partition-offsets " partition-offsets)

    (if (= true (:use-earliest conf)) (apply min partition-offsets) (apply max partition-offsets))))

(defn- topic-partition? [m topic partition] (filter (fn [[t partition-data]] (and (= topic t) (not-empty (filter #(= (:partition %) partition) partition-data)))) m))

(defn get-broker-from-meta [{:keys [conf] :as state} topic partition & {:keys [retry-count] :or {retry-count 0}}]
  (let [meta (meta/get-metadata! state conf)
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
  [{:keys [metadata-producers-ref conf error-handler] :as state} topics]
  {:pre [metadata-producers-ref conf]}
  (try
    (let [meta (meta/get-metadata! state conf)
          offsets (cutil/get-broker-offsets state meta topics conf)]

      (doseq [[broker topic-data] offsets]
        (doseq [[topic offset-data] topic-data]
          (try
            ;we map :offset to max of :offset and :all-offets
            (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
            (catch Exception e (do (error e e) (.printStackTrace e) (if error-handler (error-handler :meta state e)))))))
      state)
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)
                         state))))

(defn create-meta-producers!
  "For each boostrap broker a metadata-request-producer is created"
  [bootstrap-brokers conf]
  (meta/bootstrap->producermap bootstrap-brokers conf))

(defn create-organiser!
  "Create a organiser state that should be passed to all the functions were state is required in this namespace"
  [{:keys [bootstrap-brokers work-queue working-queue complete-queue redis-conf redis-factory conf] :or {redis-factory redis/create} :as state}]
  {:pre [work-queue working-queue complete-queue
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port)]}

  (let [shutdown-flag (AtomicBoolean. false)
        shutdown-confirm (CountDownLatch. 1)
        metadata-producers-ref (ref (create-meta-producers! bootstrap-brokers conf))
        blacklisted-metadata-producers-ref (ref (cache/create-cache :expire-after-write (get conf :blacklisted-expire 10000)))
        redis-conn (redis-factory redis-conf)
        intermediate-state (assoc state :metadata-producers-ref metadata-producers-ref
                                        :blacklisted-metadata-producers-ref blacklisted-metadata-producers-ref
                                        :redis-conn redis-conn :offset-producers (ref {}) :shutdown-flag shutdown-flag :shutdown-confirm shutdown-confirm)
        work-complete-processor-future (start-work-complete-processor! intermediate-state)
        unblack-list-processor-future (start-metadata-unblacklist-processor! metadata-producers-ref blacklisted-metadata-producers-ref conf)
        state (assoc intermediate-state
                :unblack-list-processor-future unblack-list-processor-future
                :work-assigned-flag (atom 0)
                :work-complete-processor-future work-complete-processor-future)]

    ;;if no metadata throw an exception and close the organiser
    (when (nil? (meta/get-metadata! state conf))
      (try
        (close-organiser! state)
        (catch Exception e (error e e)))
      (throw (ex-info (str "No metadata could be found from any of the bootstrap brokers provided " bootstrap-brokers) {:type              :metadata-exception
                                                                                                                        :bootstrap-brokers bootstrap-brokers})))
    state))


(defn wait-on-work-assigned-flag [{:keys [work-assigned-flag]} timeout-ms]
  (loop [ts (System/currentTimeMillis)]
    (if (> @work-assigned-flag 0)
      @work-assigned-flag
      (if (>= (- (System/currentTimeMillis) ts) timeout-ms)
        -1
        (do
          ;(prn "waiting on work-assigned-flag: " @work-assigned-flag)
          (Thread/sleep 1000)

          (recur (System/currentTimeMillis)))))))

(defn close-organiser!
  "Closes the organiser passed in"
  [{:keys [metadata-producers-ref work-complete-processor-future unblack-list-processor-future
           redis-conn ^AtomicBoolean shutdown-flag ^CountDownLatch shutdown-confirm]} & {:keys [close-redis] :or {close-redis true}}]
  {:pre [metadata-producers-ref redis-conn
         (instance? ExecutorService work-complete-processor-future)
         (instance? ExecutorService unblack-list-processor-future)]}
  (.set shutdown-flag true)
  (.await shutdown-confirm 10000 TimeUnit/MILLISECONDS)
  (.shutdownNow ^ExecutorService unblack-list-processor-future)
  (.shutdown ^ExecutorService work-complete-processor-future)

  (doseq [[_ producer] @metadata-producers-ref]
    (when (realized? producer)                              ;only close a producer if it was realised
      (produce/shutdown @producer)))

  (when close-redis
    (redis/close! redis-conn)))
