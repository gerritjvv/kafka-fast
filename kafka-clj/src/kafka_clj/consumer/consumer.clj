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
        (info "Messages read " msg-read " discarded " discarded " check max-bytes"))
      state)))

(defn- fetchstate->state-tuple
      "Converts a FetchState into (tuple status offset)"
      [^FetchState state]
      (when (print-discarded state)
        (tuple (.getStatus state) (.getOffset state))))

(extend-protocol IMsgEvent
  Fetch$Message                                           ;topic partition offset bts
  (-msg-event [{:keys [^long offset] :as msg} ^FetchState state]
    (if (and (< offset (.getMaxOffset state)) (> offset (.getOffset state)))
      (do
        ((.getDelegate state) msg)
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
                                                             (handler-f wu)))))
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
   Returns [status offset]
   Throws: Exception, may block"
  [delegate-f wu ^"[B" bts]
  {:pre [(fn? delegate-f)]}
  (io!
    (fetchstate->state-tuple                                ;convert FetchState to [status offset]
      (Fetch/readFetchResponse
        (tcp/wrap-bts bts)
        (fetch-state delegate-f wu)         ;mutable FetchState
        handle-msg-event))))

(defn- process-wu!
  " Borrow a connection
    Write a fetch request
    Process the fetch request sending all messages to delegate-f
    Publish the wu as consumed to redis, or on error as error"
  [state conn-pool delegate-f wu]
  {:pre [conn-pool (get-in wu [:producer :host]) [get-in wu [:producer :port]]]}
  (try

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
                [status offset] (read-process-resp! delegate-f wu bts)]
            (if (= :ok status)
              (wu-api/publish-consumed-wu! state wu (if (pos? offset) offset (:offset wu)))
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
                                       (wu-api/publish-zero-consumed-wu! state wu)))))


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

          wu-processor (partial process-wu! state conn-pool delegate-f)]

      @reporter

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