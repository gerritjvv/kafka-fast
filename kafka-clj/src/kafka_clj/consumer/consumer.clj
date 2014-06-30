(ns kafka-clj.consumer.consumer
  (:import (java.util.concurrent ExecutorService)
           (clojure.lang ArityException))

  (:require 
    [taoensso.carmine :as car :refer [wcar]]
    [thread-load.core :as load]
    [clojure.core.async :refer [go alts!! >!! >! <! timeout chan]]
    [kafka-clj.fetch :refer [create-fetch-producer send-fetch read-fetch]]
    [thread-load.core :as tl]
    [clojure.data.json :as json]
    [clj-tuple :refer [tuple]]
    [fun-utils.core :refer [go-seq]]
    [clojure.tools.logging :refer [info error debug]])
  (:import 
    [kafka_clj.fetch Message FetchError]
    [java.util.concurrent Executors ExecutorService]
    [clj_tcp.client Reconnected Poison]
    [com.codahale.metrics Meter MetricRegistry Timer Histogram]
    [clojure.lang ArityException]
    [io.netty.buffer Unpooled]))

;;; This namespace requires a running redis and kafka cluster
;;;;;;;;;;;;;;;;;; USAGE ;;;;;;;;;;;;;;;
;(use 'kafka-clj.consumer.consumer :reload)
;(def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))
;
;
;
;(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
;
;
;(def res (wait-and-do-work-unit! consumer))
;
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(defonce byte_array_class (Class/forName "[B"))
(defn- byte-array? [arr] (instance? byte_array_class arr))

(defn read-fetch-message 
  "read-fetch will return the result of fn which is [resp-vec error-vec]"
  [{:keys [topic partition ^Long offset ^Long len]} f-delegate v]
  (if (byte-array? v)
	  (let [ ^Long max-offset (+ offset len)
	         fetch-res
	         (read-fetch (Unpooled/wrappedBuffer ^"[B" v) [[] [] nil]
				     (fn [state msg]
	              ;read-fetch will navigate the fetch response calling this function
               ;(info "READ FETCH MESSAGE " msg)
	              (if (coll? state)
			            (let [[resp errors f-state] state]
		               (try
			               (do 
					             (cond
								         (instance? Message msg)
                         ;only include messsages of the same topic partition and lower than max-offset
								         (do
                           ;(spit "/d1/kafka-clj-tests1.txt" "test\n" :append true)
                           (if (and (= (:topic msg) topic) (= ^Long (:partition msg) ^Long partition) (< ^Long (:offset msg) max-offset))
                             (tuple (conj resp msg) errors (f-delegate f-state msg))
                             (tuple resp errors f-state)))
								         (instance? FetchError msg)
								         (do (error "Fetch error: " msg) (tuple resp (conj errors msg) f-state))
								         :else (throw (RuntimeException. (str "The message type " msg " not supported")))))
			               (catch Exception e 
		                  (do (error e e)
		                      (tuple resp errors f-state))
		                  )))
	                  (do (error "State not supported " state)
	                      [{} [] nil])
	                  )))]
         ;(info "FETCH RESP " fetch-res)
	       (if (coll? fetch-res)
	          fetch-res
		       (do
		         (info "No messages consumed " fetch-res)
		         nil)))))

(defn handle-error-response [v]
  [:fail v])

(defn handle-read-response [work-unit f-delegate v]
  (let [[resp-vec error-vec] (read-fetch-message work-unit f-delegate v)]
    [:ok resp-vec]))

(defn handle-timeout-response []
  [:fail nil])


(defn handle-response
  "Listens to a response after a fetch request has been sent
   Returns [status data]  status can be :ok, :timeout, :error and data is v returned from the channel"
  [{:keys [client] :as state} work-unit f-delegate conf]
  ;(prn "handler-response >>>>> " work-unit)
  (let [fetch-timeout (get conf :fetch-timeout 10000)
        {:keys [read-ch error-ch]} client
        [v c] (alts!! [read-ch error-ch (timeout fetch-timeout)])]
    (condp = c
      read-ch (cond 
                (instance? Reconnected v) (handle-response state f-delegate conf)
                (instance? Poison v) [:fail nil]
                :else (handle-read-response work-unit f-delegate v))
      error-ch (handle-error-response v)
      (handle-timeout-response))))

    
(defn fetch-and-wait 
  "
   Sends a request for the topic and partition to the producer
   Returns [status data]  status can be :ok, :fail and data is v returned from the channel"
  [state {:keys [topic partition offset len] :as work-unit} producer f-delegate]
    (io!
      (send-fetch producer [[topic [{:partition partition :offset offset}]]])
      (handle-response producer work-unit f-delegate (get state :conf))))


(defn- safe-sleep
  "Util function that does not print an Interrupted exception but handles by setting the current thread to interrupt"
  [ms]
  (try
    (Thread/sleep ms)
    (catch InterruptedException i (doto (Thread/currentThread) (.interrupt)))))

(defn wait-on-work-unit!
  "Blocks on the redis queue till an item becomes availabe, at the same time the item is pushed to the working queue"
  [work-unit-event-ch redis-conn queue working-queue]
  (if-let [res (try                                         ;this command throws a SocketTimeoutException if the queue does not exist
                 (car/wcar redis-conn                       ;we check for this condition and continue to block
                           (car/brpoplpush queue working-queue 0))
                 (catch java.net.SocketTimeoutException e (do (safe-sleep 1000) (debug "Timeout on queue " queue " retry ") nil)))]
    res
    (recur work-unit-event-ch redis-conn queue working-queue)))

(defn consumer-start
  "Starts a consumer and returns the consumer state that represents the consumer itself
   A msg-ch can be provided but if not a (chan 100) will be created and assigned to msg-ch in the state.
   keys are:
           :redis-conn the redis connection
           :load-pool a load pool from tl/create-pool or if a load-pool exists in the state the same load pool is used
           :msg-ch the core.async channel
           :producers {}
           :status :ok
  "
  [{:keys [redis-conf conf msg-ch load-pool] :as state}]
  {:pre [(and 
           (:work-queue state) (:working-queue state) (:complete-queue state)
           (:redis-conf state) (:conf state))]}
  (merge state
    {:redis-conn {:pool {:max-active (get redis-conf :max-active 20)}
                  :spec {:host  (get redis-conf :host "localhost")
                         :port    (get redis-conf :port 6379)
                         :password (get redis-conf :password)
                         :timeout  (get redis-conf :timeout 4000)}}
     :load-pool (if load-pool load-pool (tl/create-pool :queue-limit (get conf :consumer-queue-limit 10)))
     :msg-ch (if msg-ch msg-ch (chan 100))
    :producers {}
    :status :ok}))

(defn consumer-stop [{:keys [producers work-queue working-queue] :as state}] (assoc state :status :ok))

(defn create-producer-if-needed!
  "If (get producers producer) returns nil a new producer is created.
  This function returns [producer-connection state]"
  [producers producer conf]
  (if-let [producer-conn (get producers producer)] 
    [producer-conn producers]
    (let [producer-conn  (create-fetch-producer producer conf)]
      [producer-conn (assoc producers producer producer-conn)])))

(defn publish-work-response! 
  "Remove data from the working-queue and publish to the complete-queue"
  [{:keys [redis-conn working-queue complete-queue work-queue work-unit-event-ch]} work-unit status resp-data]
  {:pre [work-unit-event-ch redis-conn working-queue complete-queue work-queue work-unit]}
  ;(info "publish-work-response! >>> " complete-queue " complete-queue " status )
  (let [work-unit2 (assoc work-unit :status status :resp-data resp-data)]
    (if work-unit-event-ch
      (>!! work-unit-event-ch                                 ;send to work-unit-event-channel
           {:event "done"
            :ts (System/currentTimeMillis)
            :wu work-unit2}))
    ;send work complete to complete-queue
    (car/wcar redis-conn
              (car/lpush complete-queue work-unit2)
              (car/lrem working-queue -1 work-unit))
    ))

(defn save-call [f state & args]
  (try
    (apply f state args)
    (catch Exception t (do (error t t) (assoc state :status :fail)))))

(defn get-work-unit!
  "Wait for work to become available in the work queue
   Adds a :seen key to the work unit with the current milliseconds"
  [{:keys [redis-conn work-queue working-queue work-unit-event-ch]}]
  {:pre [redis-conn work-queue working-queue]}
  (wait-on-work-unit! work-unit-event-ch redis-conn work-queue working-queue))

(defn- get-offset-read
  "Returns the max value in the resp data of :offset if no values 0 is returned"
  [resp-data]
  (try
    (apply max (map :offset resp-data))
    (catch ArityException e 0)))

(defn do-work-unit! 
  "state map keys:
    :redis-conn = redis connection params :host :port ... 
    :producers = (ref {}) contains the current brokers to which fetch requests can be sent, these are created dynamically
    :work-queue = the queue name from which work units will be taken, the data must be a map with keys :producer :topic :partition :offset :len
    :working-queue = when a work item is taken from the work-queue its placed on the working-queue
    :complete-queue = when an item has been processed the result is placed on the complete-queue
    :conf = any configuration that will be passed when creating producers
   f-delegate is called as (f-delegate state status resp-data) and should return a state that must have a :status key with values :ok, :fail or :terminate
   
   If the work-unit was successfully processed the work-unit assoced with :resp-data {:offset-read max-message-offset}
   and added to the complete-queue queue.
   Returns the state map with the :status and :producers updated
  "
  [{:keys [redis-conn producers work-queue working-queue complete-queue conf] :as state} work-unit f-delegate]
  ;(prn "wait-and-do-work-unit! >>> have work unit " work-unit)
  (io!
    (try
      (let [{:keys [producer topic partition offset len]} work-unit
            [producer-conn producers2] (create-producer-if-needed! producers producer conf)]
        (try
          (do
            (if (not producer-conn) (throw (RuntimeException. "No producer created")))
            (let [[status resp-data] (fetch-and-wait state work-unit producer-conn f-delegate)
                  state2 (assoc state :status :ok)
                  ;state2 (merge state (save-call f-delegate state status resp-data))
                  ]
              ;(prn "wait-and-do-work-unit! >>> publish work response resp-data" resp-data)
              (if resp-data
                (publish-work-response! state2 work-unit (:status state2) {:offset-read (get-offset-read resp-data)})
                (do
                  ;@TODO WE need to analyse why exactly the resp-data is nil here and how to prefent it by calculating the offsets better
                  (info ">>>>>>>>>>>>>> nil resp-data " resp-data  " status " status  " w-unit " work-unit)

                  ))

              (assoc
                  state2
                :producers producers2)))
          (catch Throwable t (do
                               (.printStackTrace t)
                               (publish-work-response! state work-unit :fail nil)
                               (assoc state :status :fail :throwable t :producers  producers2)))))
      (catch Throwable t (assoc state :status :fail :throwable t)))))
    
(defn wait-and-do-work-unit! 
  "Combine waiting for a workunit and performing it in one function
   The state as returned by do-work-unit! is returned"
  [state f-delegate]
  (let [work-unit (get-work-unit! state)]
    ;(prn "wait-and-do-work-unit! >>>>>>> got work")
    (do-work-unit! state work-unit f-delegate)))
    
(defn publish-work 
  "Publish a work-unit to the working queue for a consumer connection"
  [{:keys [redis-conn work-queue]} work-unit]
  {:pre [(and (:producer work-unit) (:topic work-unit) (:partition work-unit) (:offset work-unit) (:len work-unit)
           (let [{:keys [host port]} (:producer work-unit)] (and host port)))]}
  (io!
    (car/wcar redis-conn
              (car/lpush work-queue work-unit))))


(defn- ^Runnable publish-pool-loop [{:keys [load-pool] :as state}]
  (fn []
    (while (not (Thread/interrupted))
      (try
        ;@TODO add duplicate work detection here, try something like a bloom filter
        (tl/publish! load-pool (get-work-unit! state))
        (catch Exception e (error e e))))))

(defn start-publish-pool-thread 
  "Start a future that will wait for a workunit and publish to the thread-pool"
  [{:keys [load-pool] :as state}]
  {:pre [load-pool]}
  (doto (Executors/newSingleThreadExecutor) (.submit (publish-pool-loop state))))


(defn close-consumer! [{:keys [load-pool publish-pool]}]
  {:pre [load-pool (instance? ExecutorService publish-pool)]}
  (tl/shutdown-pool load-pool 10000)
  (.shutdownNow ^ExecutorService publish-pool))


(defn- close-for-restart-consumer! [{:keys [load-pool publish-pool]}]
  {:pre [load-pool (instance? ExecutorService publish-pool)]}

  )




(defn- copy-chs [ch chs]
  (go
    (loop [[v c] (alts! chs)]
      (if v
        (do
          ;(spit "/d1/kafka-clj-tests3.txt" "test\n" :append true)
          (>! ch v)
          (recur (alts! chs)))))))

(defn consume!
  "Starts the consumer consumption process, by initiating 1+consumer-threads threads, one thread is used to wait for work-units
   from redis, and the other threads are used to process the work-unit, the resp data from each work-unit's processing result is 
   sent to the msg-ch, note that the send to msg-ch is a blocking send, meaning that the whole process will block if msg-ch is full
   The actual consume! function returns inmediately

  "
  [{:keys [conf msg-ch work-unit-event-ch] :as state}]
  {:pre [conf work-unit-event-ch msg-ch
         (instance? clojure.core.async.impl.channels.ManyToManyChannel msg-ch)
         (instance? clojure.core.async.impl.channels.ManyToManyChannel work-unit-event-ch)]}
  (let [

        {:keys [load-pool] :as ret-state} (merge state (consumer-start state) {:restart 0})
        consumer-threads (get conf :consumer-threads 8)
        ;create a chan per thread, updates are faster and there is less mutex lock contention
        ch-vec (vec (for [i (range consumer-threads)] (chan 100)))

        publish-pool (start-publish-pool-thread ret-state)]
    (copy-chs msg-ch ch-vec)                                ;setup copy from threaded channels to msg-ch
    ;add threads that will consume from the load-pool and run f-delegate, that will in turn put data on the msg-ch
    (dotimes [i consumer-threads]
      (let [ch (ch-vec i)
            f-delegate2 (fn [state resp-data]
                          (>!! ch resp-data))]
        (tl/add-consumer load-pool
                         (fn [{:keys [restart] :as state} & _] ;init
                           (info "start consumer thread restart " restart)
                           (if-not restart
                             (assoc ret-state :status :ok :publish-pool publish-pool)
                             (assoc (consumer-start state) :status :ok :restart (inc restart) :publish-pool publish-pool)))
                         (fn [state work-unit] ;exec
                           (do-work-unit! state work-unit f-delegate2))
                         (fn [state & args] ;fail
                           (info "Fail consumer thread: " state " " args)
                           (if-let [e (:throwable state)] (error e e))
                           (close-for-restart-consumer! state)
                           (assoc (merge state (consumer-start state)) :status :ok)))))
    ;start background wait on redis, publish work-unit to pool

    (assoc ret-state :publish-pool publish-pool)))
    
                                   
(comment 
  
(use 'kafka-clj.consumer.consumer :reload)
(def consumer {:redis-conf {:host "localhost" :max-active 5 :timeout 1000}
               :work-unit-event-ch (chan (sliding-buffer 10))
               :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
(def res (wait-and-do-work-unit! consumer (fn [state resp-data] resp-data)))

(use 'kafka-clj.consumer.consumer :reload)

(require '[clojure.core.async :refer [go alts!! >!! <!! >! <! timeout chan]])
(def msg-ch (chan 1000))

(def consumer {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 11 :len 10})

(consume! (assoc consumer :msg-ch msg-ch))

(<!! msg-ch)
(<!! msg-ch)

)


