(ns
  ^{:author "gerritjvv"
    :doc    "Internal consumer code, for consumer public api see kafka-clj.node"}
  kafka-clj.consumer.consumer
  (:require [kafka-clj.tcp :as tcp]
            [kafka-clj.schemas :as schemas]
            [fun-utils.threads :as threads]
            [clojure.tools.logging :refer [error info warn debug]]
            [kafka-clj.fetch :as fetch]
            [clj-tuple :refer [tuple]]
            [kafka-clj.consumer.workunits :as wu-api]
            [clojure.core.async :as async]
            [kafka-clj.metadata :as kafka-metadata]
            [tcp-driver.driver :as tcp-driver]
            [tcp-driver.io.stream :as tcp-stream]
            [schema.core :as s])
  (:import (java.util.concurrent TimeUnit ExecutorService ThreadPoolExecutor ConcurrentHashMap)
           (kafka_clj.util FetchState Fetch Fetch$Message Fetch$FetchError Util)
           (clojure.core.async.impl.channels ManyToManyChannel)
           (java.net SocketException)
           (java.util.concurrent.atomic AtomicBoolean)
           (com.codahale.metrics MetricRegistry Meter ConsoleReporter)
           (java.util Map)
           (io.netty.buffer ByteBuf Unpooled)))

;;;;;;;;;;;;;;;
;;;;; Metrics
(def ^AtomicBoolean METRICS-REPORTING-STARTED (AtomicBoolean. false))
(def ^MetricRegistry metrics-registry (MetricRegistry.))
(def messages-read (.meter metrics-registry "messages-read"))

;;;;;;;;;;;;;


;;;;;;;;;;;;;;;
;;;;;; Internal top level functions used by metrics and Protocols

(defn mark!
  "Mark messages read meter"
  [^Meter meter]
  (.mark meter))

(defn mark-min-bytes! [topic minbts]
  (.update (.histogram metrics-registry (str topic "-max-bts")) (long minbts)))

(defn mark-max-bytes! [topic maxbts]
  (.update (.histogram metrics-registry (str topic "-max-bts")) (long maxbts)))

(defn ^FetchState fetch-state
  "Creates a mutable initial state"
  [delegate-f {:keys [topic partition max-offset] :as m}]
  (let [state (FetchState. delegate-f topic :ok (long partition) -1 (long max-offset))]
    (debug "fetch-state start with " m " return state " state)
    state))

(defn- update-offset! [^FetchState state ^long offset]
  (doto state (.setOffset offset)))

(defn- print-discarded [^FetchState state]
  (when state
    (let [discarded (.getDiscarded state)
          msg-read (- (.getOffset state) (.getInitOffset state))]
      (when (and (pos? msg-read) (> (int (* (/ discarded msg-read) 100)) 15))
        (warn "Messages read " msg-read " discarded " discarded " check max-bytes"))
      state)))

(defn- fetchstate->state-tuple
  "Converts a FetchState into (tuple status offset maxoffset discarded min max processed-count)"

  ;[:ok offset: 3160955437 maxoffset: 3160955438 discarded: 128633 min: 512 max: 826 processed-count: 3160955438]
  [^FetchState state]
  (when (print-discarded state)
    (tuple (.getStatus state)
           (.getOffset state)
           (.getMaxOffset state)
           (.getDiscarded state)
           (.getMinByteSize state)
           (.getMaxByteSize state)
           (- (.getOffset state) (.getInitOffset state)))))

;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; Message Protocols
;;;;;;;;;;
;;;;;;;;;;  When a message is received from the Fetch process it can either be a Message or Fetch Error
;;;;;;;;;;  How the message is handled is taken care of through the IMsgEven protocol

(defprotocol IMsgEvent
  "Simplifies the logic of processing a FetchError and normal Message instance from a broker fetch response"
  (-msg-event [msg state] "Must return FetchState status can be :ok or :error"))

(extend-protocol IMsgEvent
  Fetch$Message                                             ;topic partition offset bts
  (-msg-event [{:keys [^long offset ^"[B" bts] :as msg} ^FetchState state]

    (if (and (< offset (.getMaxOffset state)) (> offset (.getOffset state)))
      (do
        ((.getDelegate state) msg)
        (.updateMinMax state bts)
        (update-offset! state offset))
      (do
        (.incDiscarded state)
        state)))

  Fetch$FetchError
  (-msg-event [{:keys [error-code] :as msg} ^FetchState state]
    (error (.toString ^Fetch$FetchError msg))
    (doto state (.setStatus (if (#{1 3} error-code) :fail-delete :fail)))))

;;;;;;;;;;;;;;;;;;;
;;;;;; Private Functions

(defn- thread-pool-stats
  "Return the stats of the ExecutorService depending on its type
   currently only ThreadPoolExecutor is supported"
  [^ExecutorService exec]
  (if (instance? ThreadPoolExecutor exec)
    {:active-count             (.getActiveCount ^ThreadPoolExecutor exec)
     :core-pool-size           (.getCorePoolSize ^ThreadPoolExecutor exec)
     :pool-size                (.getPoolSize ^ThreadPoolExecutor exec)
     :queue-remaining-capacity (-> exec .getQueue .remainingCapacity)}
    {}))


(defn- start-wu-publisher! [state publish-exec-service exec-service handler-f]
  (let [^AtomicBoolean shutdown-flag (:shutdown-flag state)]
    (threads/submit publish-exec-service
                    (fn []
                      (while (and (not (Thread/interrupted)) (not (.get shutdown-flag)))
                        (try
                          (let [wu (wu-api/get-work-unit! state)]
                            (if wu
                              (threads/submit exec-service (fn []
                                                             (try
                                                               (handler-f wu)
                                                               (catch Exception e (error e e)))))))
                          (catch InterruptedException ie (do
                                                           (.printStackTrace ie)
                                                           (.interrupt (Thread/currentThread))))
                          (catch IllegalStateException e
                            (.printStackTrace e)
                            (error e e)
                            (.interrupt (Thread/currentThread)))
                          (catch InterruptedException _ nil)
                          (catch Exception e (do
                                               (.printStackTrace e)
                                               (error e e)))))
                      (info "EXIT publisher loop!!")))))

(defn- handle-msg-event
  "Monoid
   Returns FetchState"
  ([state msg]
    ;;;see IMsgEvent Protocol
   (-msg-event msg state)))

(defn read-process-resp!
  "Reads the response from the TCP conn, then calls fetch/read-fetch and process with handle-msg
   Returns [status offset discarded min max]
   Throws: Exception, may block"
  [delegate-f wu ^"[B" bts]
  {:pre [(fn? delegate-f)]}
  (io!
    (fetchstate->state-tuple                                ;convert FetchState to [status offset discarded min max]
      (Fetch/readFetchResponse
        (tcp/wrap-bts bts)
        (fetch-state delegate-f wu)                         ;mutable FetchState
        handle-msg-event))))

(defn write-rcv-fetch-req! [state delegate-f {:keys [partition topic offset] :as wu} conn]
  {:pre [(s/validate schemas/WORK-UNIT-SCHEMA wu)]}
  (locking conn
    (let [timeout-ms 60000
          ^ByteBuf buff (Unpooled/buffer)
          _ (do (fetch/write-fetch-request buff [[topic [{:partition partition :offset offset}]]] state))

          _ (tcp-stream/write-bytes conn (Util/toBytes buff))

          _ (tcp-stream/flush-out conn)

          msg-len (tcp-stream/read-int conn timeout-ms)

          resp (tcp-stream/read-bytes conn msg-len timeout-ms)]

      (read-process-resp! delegate-f wu ^"[B" resp))))

(defn process-wu!
  " Borrow a connection
    Write a fetch request
    Process the fetch request sending all messages to delegate-f
    Publish the wu as consumed to redis, or on error as error"
  [state metadata-connector delegate-f wu]
  {:pre [(:driver metadata-connector)
         (s/validate schemas/WORK-UNIT-SCHEMA, wu)]}
  (try

    (debug "processing-wu " wu)

    (let [timeout-ms 60000

          ;;[status offset maxoffset discarded minbts maxbts offsets-read :as v]
          [status offset _ _ _ _ offsets-read :as v] (tcp-driver/send-f (:driver metadata-connector)
                                                                        (:producer wu) ;;send specifically to the producer :host :port for the work unit
                                                                        (partial write-rcv-fetch-req! state delegate-f wu)
                                                                        timeout-ms)]

      (debug "got fetch response: static: " status " offset: " offset " offsets-read:" v)

      (if (= :ok status)
        (do
          (if (zero? (long offsets-read))
            (wu-api/publish-zero-consumed-wu! state wu)     ;of no offsets were read, we need to mark the wu as zero consumed
            (wu-api/publish-consumed-wu! state wu (if (pos? offset) offset (:offset wu))))
          v)
        (do
          (wu-api/publish-error-wu! state wu status offset)
          nil)))
    (catch Throwable ne (do
                          (.printStackTrace ne)
                          (error ne ne)
                          (wu-api/publish-zero-consumed-wu! state wu)
                          nil))))


(defonce ^Long TWENTY-MEGS 20971520)
(defonce ^Long TWO-MEGS 2097152)
(defonce ^Long ONE-KB 1024)

(defonce DEFAULT-MAX-BTS-REM [7340032 (* 1024 512) (* 1024 512) (System/currentTimeMillis)])

(defn update-state-max-bytes [state max-bytes]
  (if (and max-bytes (pos? max-bytes))
    (assoc-in state [:conf :max-bytes] max-bytes)
    state))

(defn over-seconds-ago? [^long time-ms ^long seconds]
  (> (- (System/currentTimeMillis) time-ms) (* seconds 1000)))

(defn increase-val
  "increase the value v by (/ v decayer) up to a max of two megs,
   if v is already MAX or the values has been updated more than 60 seconds ago RESET-VAL is returned to reset the value"
  [v decayer MAX RESET-VAL updates-ts]
  (if (or (>= (long v) (long MAX)) (over-seconds-ago? (long updates-ts) 60))
    RESET-VAL
    (Math/min (long (+ v (Math/ceil (/ v decayer)))) (long MAX))))

(defn increase-inc
  "Increase the incrementing value by half its size up to two megs,
  if the value is already TWO-MEGS long its reset to 1KB"
  [^long v ^long update-ts]
  (increase-val v 2 TWO-MEGS ONE-KB update-ts))

(defn increase-dec
  "Increase the decrementing value by a quarter of its size up to two megs,
  if the value is already TWO-MEGS long its reset to 1KB"
  [^long v ^long update-ts]
  (increase-val v 4 TWO-MEGS ONE-KB update-ts))

(defn update-dec-rem
  "Update the memory set containing [max-bts incrementor decrementor updated-ts], increasing the decrementor"
  [[max-bts rem-inc rem-dec updated-ts]]
  (tuple (Math/max 512 (long (- (long max-bts) (long rem-dec))))
         rem-inc
         (increase-dec rem-dec updated-ts)
         (System/currentTimeMillis)))

(defn update-inc-rem
  "Update the memory set containing [max-bts incrementor decrementor updated-ts] increasing the incrementor"
  [[max-bts rem-inc rem-dec updated-ts]]

  (tuple (Math/min (long TWENTY-MEGS) (long (+ (long max-bts) (long rem-dec))))
         (increase-inc rem-inc updated-ts)
         rem-dec
         (System/currentTimeMillis)))


(defn with-default
  "Apply the function to its argument only if the argument is not nil, otherwise default-val is returned"
  [f default-val]
  (fn [arg]
    (if arg
      (f arg)
      default-val)))

(defn auto-tune-fetch
  "Wraps around the process-wu! function and use the return state to calculate what the max-bytes in up comming fetch requests should be.
   "
  [max-bytes-at state metadata-connector delegate-f wu]
  {:pre [metadata-connector]}

  (try
    (let [topic (:topic wu)
          partition (:partition wu)

          [rem-max-bts _ _] (get-in @max-bytes-at [topic partition] DEFAULT-MAX-BTS-REM)

          [_ offset maxoffset discarded minbts maxbts _] (process-wu!
                                                           (update-state-max-bytes state rem-max-bts) ;;here we add :conf :max-bytes to state
                                                           metadata-connector
                                                           delegate-f
                                                           wu)]
      (when discarded                                       ;test that any of the items exist and that nil wasn't returned from process-wu!
        (mark-min-bytes! topic minbts)
        (mark-max-bytes! topic maxbts)

        (debug "max-bytes-at " @max-bytes-at)

        (let [update-f (cond
                         (> (long discarded) 5)
                         (do
                           (debug "Adjusting dec " topic `" " partition "  max-bytes discarded " discarded " minbts " minbts " maxbts " maxbts " maxoffset " maxoffset " offset " offset)
                           update-dec-rem)
                         (> (- maxoffset offset) 5)
                         (do
                           (debug "Adjusting inc " topic " " partition "  max-bytes discarded " discarded " minbts " minbts " maxbts " maxbts)
                           update-inc-rem))]

          (when update-f
            (swap! max-bytes-at #(update-in % [topic partition] (with-default update-f DEFAULT-MAX-BTS-REM)))))))
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)))))

;;;;;;;;;;;;;;;;
;;;;;;;; Public Functions

(defn start-metrics-reporting!
  "Start the metrics reporting, writing to STDOUT every 10 seconds"
  []
  (when (not (.getAndSet METRICS-REPORTING-STARTED true))
    (-> (ConsoleReporter/forRegistry metrics-registry)
        (.convertRatesTo TimeUnit/SECONDS)
        (.convertDurationsTo TimeUnit/MILLISECONDS)
        .build
        (.start 10 TimeUnit/SECONDS))))

(defn update-work-unit-thread-stats!
  "Update the map work-unit-thread-stats with key=<thread-name> value={:ts <timestamp> wu: <work-unit> :duration <ts-ms>}"
  [^Map work-unit-thread-stats start-ts end-ts wu]
  (.put work-unit-thread-stats (.getName (Thread/currentThread)) {:ts       start-ts
                                                                  :duration (- (long end-ts) (long start-ts))
                                                                  :wu       wu})
  work-unit-thread-stats)

(defn consume!
  "Starts the consumer consumption process, by initiating redis-fetch-threads(default 1)+consumer-threads threads, one thread is used to wait for work-units
   from redis, and the other threads are used to process the work-unit, the resp data from each work-unit's processing result is
   sent to the msg-ch, note that the send to msg-ch is a blocking send, meaning that the whole process will block if msg-ch is full
   The actual consume! function returns inmediately

    reporting: if (get :consumer-reporting conf) is true then messages consumed metrics will be written every 10 seconds to stdout

    :pool-limit 20 ; number of tcp pool connections to create

    :jaas if set the jaas authentication will be used with each tcp connection
          this value should point to the jaas config file.
          for more information see http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/AcnOnly.html
  "
  [{:keys [conf msg-ch work-unit-event-ch metadata-connector] :as state}]
  {:pre [metadata-connector
         conf
         work-unit-event-ch
         msg-ch
         (instance? ManyToManyChannel msg-ch)
         (instance? ManyToManyChannel work-unit-event-ch)]}

  (when (get conf :consumer-reporting)
    (start-metrics-reporting!))

  (io!
    (let [
          ;;shows last work-unit processed by a consumer thread key=<thread-name> value=<work-unit>
          work-unit-thread-stats (ConcurrentHashMap.)

          redis-fetch-threads (get conf :redis-fetch-threads 1)
          consumer-threads (get conf :consumer-threads 2)

          publish-exec-service (threads/create-exec-service redis-fetch-threads)
          shutdown-flag (AtomicBoolean. false)

          exec-service (threads/create-exec-service consumer-threads)
          delegate-f (if (get conf :consumer-reporting)
                       (fn [msg]
                         (async/>!! msg-ch msg)
                         (mark! messages-read))
                       (fn [msg]
                         (async/>!! msg-ch msg)))

          max-bytes-at (atom {})

          ;;call update work-unit-stats and return the value of auto-tune-fetch
          wu-processor (fn [wu]
                         (let [start-ts (System/currentTimeMillis)
                               v (auto-tune-fetch max-bytes-at state metadata-connector delegate-f wu)]

                           ;;update stats!
                           (update-work-unit-thread-stats! work-unit-thread-stats start-ts (System/currentTimeMillis) wu)

                           ;;return auto-tune-fetch result
                           v))]

      ;;for each fetch thread we start a fetcher on the publish-exec-service
      (dotimes [_ redis-fetch-threads]
        (start-wu-publisher! (assoc state :shutdown-flag shutdown-flag) publish-exec-service exec-service wu-processor))

      (assoc state
        :publish-exec-service publish-exec-service
        :exec-service exec-service
        :metadata-connector metadata-connector
        :shutdown-flag shutdown-flag
        :work-unit-thread-stats work-unit-thread-stats))))


(defn show-work-unit-thread-stats
  "
  public function
  Return the work-unit-thread-stats that show key=thread value={:ts <the time the wu was seen> :duration <time it took for fetch> :wu <work-unit>}"
  [{:keys [work-unit-thread-stats]}]
  work-unit-thread-stats)

(defn consumer-pool-stats
  "Return a stats map for instances returned from the consume! function"
  [{:keys [^ExecutorService exec-service] :as conn}]
  {:exec-service (thread-pool-stats exec-service)

   :fetch-stats  (show-work-unit-thread-stats conn)})

(defn close-consumer! [{:keys [publish-exec-service exec-service metadata-connector ^AtomicBoolean shutdown-flag]}]
  (.set shutdown-flag true)
  (info "closing publish-exec-serivce")
  (threads/close! {:executor publish-exec-service} :timeout-ms 30000)
  (info "closing exec-serivce")
  (threads/close! {:executor exec-service} :timeout-ms 30000)
  (info "closing metadata-connector")
  (kafka-metadata/close metadata-connector)
  (info "all consumer resources closed"))