(ns
  ^{:author "gerritjvv"
    :doc "Internal consumer code, for consumer public api see kafka-clj.node"}
  kafka-clj.consumer.consumer
  (:require [kafka-clj.tcp :as tcp]
            [fun-utils.threads :as threads]
            [clojure.tools.logging :refer [error info warn]]
            [kafka-clj.fetch :as fetch]
            [clj-tuple :refer [tuple]]
            [kafka-clj.consumer.workunits :as wu-api]
            [clojure.core.async :as async]
            [clojure.tools.logging :refer [info]])
  (:import (java.util.concurrent Executors TimeUnit)
           (kafka_clj.util FetchState Fetch Fetch$Message Fetch$FetchError)
           (clojure.core.async.impl.channels ManyToManyChannel)
           (java.net SocketException)
           (java.util.concurrent.atomic AtomicBoolean)
           (com.codahale.metrics MetricRegistry Meter ConsoleReporter)))

(def ^AtomicBoolean METRICS-REPORTING-STARTED (AtomicBoolean. false))
(def ^MetricRegistry metrics-registry (MetricRegistry.))
(def messages-read (.meter metrics-registry "messages-read"))

(defn mark!
  "Mark messages read meter"
  [^Meter meter]
  (.mark meter))

(defn mark-min-bytes! [topic minbts]
  (.update (.histogram metrics-registry (str topic "-max-bts")) (long minbts)))

(defn mark-max-bytes! [topic maxbts]
  (.update (.histogram metrics-registry (str topic "-max-bts")) (long maxbts)))

(defprotocol IMsgEvent
  "Simplifies the logic of processing a FetchError and normal Message instance from a broker fetch response"
  (-msg-event [msg state] "Must return FetchState status can be :ok or :error"))


(defn ^FetchState fetch-state
  "Creates a mutable initial state"
  [delegate-f {:keys [topic partition max-offset] :as m}]
  (FetchState. delegate-f topic :ok (long partition) -1 (long max-offset)))

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
      [^FetchState state]
      (when (print-discarded state)
        (tuple (.getStatus state) (.getOffset state) (.getMaxOffset state) (.getDiscarded state) (.getMinByteSize state) (.getMaxByteSize state) (.getProcessed state))))

(extend-protocol IMsgEvent
  Fetch$Message                                           ;topic partition offset bts
  (-msg-event [{:keys [^long offset ^"[B" bts] :as msg} ^FetchState state]

    (.incProcessed state)

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

(defn- write-fetch-req!
  "Write a fetch request to the connection based on wu"
  [{:keys [conf]} conn {:keys [topic partition offset] :as wu}]
  (fetch/send-fetch {:client conn :conf conf} [[topic [{:partition partition :offset offset}]]]))

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
        (fetch-state delegate-f wu)         ;mutable FetchState
        handle-msg-event))))

(defn process-wu!
  " Borrow a connection
    Write a fetch request
    Process the fetch request sending all messages to delegate-f
    Publish the wu as consumed to redis, or on error as error"
  [state conn-pool delegate-f wu]
  {:pre [conn-pool (get-in wu [:producer :host]) [get-in wu [:producer :port]]]}
  (try

    (info "Processing wu! " wu)

    (let [{:keys [host port]} (:producer wu)
          conn (tcp/borrow conn-pool host port)]

      (try
        (do
          (try
            (do
              (write-fetch-req! state conn wu))
            (catch Throwable e (do
                                   (error e e)
                                   (wu-api/publish-error-consumed-wu! state wu))))
          (let [bts (tcp/read-response wu conn 60000)
                _ (tcp/release conn-pool host port conn)     ;release the connection early
                [status offset maxoffset discarded minbts maxbts :as v] (read-process-resp! delegate-f wu bts)]
            (info ">>>> [OMG] v " v)
            (if (= :ok status)
              (do
                (wu-api/publish-consumed-wu! state wu (if (pos? offset) offset (:offset wu)))
                v)
              (wu-api/publish-error-wu! state wu status offset))))
        (catch SocketException _ (do
                                   ;socket exceptions are common when brokers fall down, the best we can do is repush the work unit so that a new broker is found for it depending
                                   ;on the metadata
                                    (warn "Work unit " wu " refers to a broker " host port " that is down, pushing back for reconfiguration")
                                    (wu-api/publish-error-consumed-wu! state wu)
                                    (tcp/release conn-pool host port conn)))
        (catch Throwable e (do (.printStackTrace e)
                               (error e e)
                               (tcp/release conn-pool host port conn)

                               (wu-api/publish-error-consumed-wu! state wu)))))
    (catch Throwable ne (do
                                       (.printStackTrace ne)
                                       (error ne ne)
                                       (wu-api/publish-zero-consumed-wu! state wu)))))


(defonce ^Long TWENTY-MEGS 20971520)

(defn calc-adjustment [total-processed discarded minbts maxbts]
  (long (* (Math/ceil (/ (if (pos? discarded) (long discarded) 1) 2))
           (Math/ceil (/ (+ (long maxbts) (long minbts)) 2 total-processed)))))

(defn update-state-max-bytes [state max-bytes]
  (if (and max-bytes (pos? max-bytes))
    (assoc-in state [:conf :max-bytes] max-bytes)
    state))

(defn nil-safe-add [a b]
  (cond
    (and a b) (+ (long a) (long b))
    a a
    :else b))

(defn nil-safe-sub [a b]
  (cond
    (and a b) (- (long a) (long b))
    a a
    :else b))

(defn auto-tune-fetch
  "Wraps around the process-wu! function and use the return state to calculate what the max-bytes in up comming fetch requests should be.
   "
  [max-bytes-at state conn-pool delegate-f wu]
  (try
    (let [[_ offset maxoffset discarded minbts maxbts total-processed] (process-wu! (update-state-max-bytes state (get @max-bytes-at (:topic wu))) conn-pool delegate-f wu)]
      (mark-min-bytes! (:topic wu) minbts)
      (mark-max-bytes! (:topic wu) maxbts)

      (info ">>> maxoffset - offset " (- maxoffset offset))
      (let [update-f  (when discarded
                        (cond
                          (> (long discarded) 5)
                          (do
                            (info "Adjusting inc " (:topic wu) "  max-bytes discarded " discarded " minbts " minbts " maxbts " maxbts)
                            #(let [x (nil-safe-add % (calc-adjustment total-processed discarded minbts maxbts))]
                              (info ">>>>> inc calculated " x)
                              (if (> x TWENTY-MEGS)
                                TWENTY-MEGS
                                x)))
                          (> (- maxoffset offset) 5)
                          (do
                            (info "Adjusting dec " (:topic wu) "  max-bytes discarded " discarded " minbts " minbts " maxbts " maxbts " maxoffset " maxoffset " offset " offset)
                            #(let [x (nil-safe-sub % (calc-adjustment total-processed discarded minbts maxbts))]
                              (info (str ">>>>> dec calculated " x))
                              (if (pos? x)
                                x
                                %)))))]

        (when update-f
          (swap! max-bytes-at #(update-in % [(:topic wu)] update-f)))))
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)))))

(defn start-metrics-reporting!
  "Start the metrics reporting, writing to STDOUT every 10 seconds"
  []
  (when (not (.getAndSet METRICS-REPORTING-STARTED true))
    (-> (ConsoleReporter/forRegistry metrics-registry)
        (.convertRatesTo TimeUnit/SECONDS)
        (.convertDurationsTo TimeUnit/MILLISECONDS)
        .build
        (.start 10 TimeUnit/SECONDS))))

(defn consume!
  "Starts the consumer consumption process, by initiating 1+consumer-threads threads, one thread is used to wait for work-units
   from redis, and the other threads are used to process the work-unit, the resp data from each work-unit's processing result is
   sent to the msg-ch, note that the send to msg-ch is a blocking send, meaning that the whole process will block if msg-ch is full
   The actual consume! function returns inmediately

    reporting: if (get :consumer-reporting conf) is true then messages consumed metrics will be written every 10 seconds to stdout
  "
  [{:keys [conf msg-ch work-unit-event-ch] :as state}]
  {:pre [conf work-unit-event-ch msg-ch
         (instance? ManyToManyChannel msg-ch)
         (instance? ManyToManyChannel work-unit-event-ch)]}

  (when (get conf :consumer-reporting)
    (start-metrics-reporting!))

  (io!
    (let [publish-exec-service (Executors/newSingleThreadExecutor)
          shutdown-flag (AtomicBoolean. false)
          conn-pool (tcp/tcp-pool conf)
          consumer-threads (get conf :consumer-threads 2)
          exec-service (threads/create-exec-service consumer-threads)
          delegate-f (if (get conf :consumer-reporting)
                       (fn [msg]
                         (async/>!! msg-ch msg)
                         (mark! messages-read))
                       (fn [msg]
                         (async/>!! msg-ch msg)))

          max-bytes-at (atom {})
          wu-processor (partial auto-tune-fetch max-bytes-at state conn-pool delegate-f)]

      (start-wu-publisher! (assoc state :shutdown-flag shutdown-flag)  publish-exec-service exec-service wu-processor)
      (assoc state :publish-exec-service publish-exec-service :exec-service exec-service :conn-pool conn-pool :shutdown-flag shutdown-flag))))

(defn close-consumer! [{:keys [publish-exec-service exec-service conn-pool ^AtomicBoolean shutdown-flag]}]
  (.set shutdown-flag true)
  (info "closing publish-exec-serivce")
  (threads/close! {:executor publish-exec-service} :timeout-ms 30000)
  (info "closing exec-serivce")
  (threads/close! {:executor exec-service} :timeout-ms 30000)
  (info "closing conn-pool")
  (tcp/close-pool! conn-pool)
  (info "all consumer resources closed"))