(ns kafka-clj.consumer.consumer
  
  (:require 
    [taoensso.carmine :as car :refer [wcar]]
    [thread-load.core :as load]
    [clojure.core.async :refer [go alts!! >! <! timeout]]
    [kafka-clj.fetch :refer [create-fetch-producer send-fetch read-fetch]]
    [clj-tuple :refer [tuple]]
    [fun-utils.core :refer [go-seq]]
    [clojure.tools.logging :refer [info error]])
  (:import 
    [kafka_clj.fetch Message FetchError]
    [clj_tcp.client Reconnected Poison]
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
;(def res (do-work-unit! consumer))
;
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(defonce byte_array_class (Class/forName "[B"))
(defn- byte-array? [arr] (instance? byte_array_class arr))

(defn read-fetch-message 
  "read-fetch will return the result of fn which is [resp-vec error-vec]"
  [{:keys [topic partition offset len]} v]
  (if (byte-array? v)
	  (let [ max-offset (+ offset len)
	         fetch-res
	         (read-fetch (Unpooled/wrappedBuffer ^"[B" v) [{} [] 0]
				     (fn [state msg]
	              ;read-fetch will navigate the fetch response calling this function
	              (if (coll? state)
			            (let [[resp errors cnt] state]
		               (try
			               (do 
					             (cond
								         (instance? Message msg)
                         ;only include messsages of the same topic partition and lower than max-offset
								         (if (and (= (:topic msg) topic) (= (:partition msg) partition) (< (:offset msg) max-offset))
                           (let [k #{(:topic msg) (:partition msg)}]
                             (tuple (assoc resp k msg) errors (inc cnt)))
                           (tuple resp errors))
								         (instance? FetchError msg)
								         (do (error "Fetch error: " msg) (tuple resp (conj errors msg) cnt))
								         :else (throw (RuntimeException. (str "The message type " msg " not supported")))))
			               (catch Exception e 
		                  (do (error e e)
		                      (tuple resp errors cnt))
		                  )))
	                  (do (error "State not supported " state)
	                      [{} [] 0])
	                  )))]
	       (if (coll? fetch-res)
	          (let [[resp errors cnt] fetch-res]
		          (tuple (vals resp) errors)) ;[resp-map error-vec]
		       (do
		         (info "No messages consumed " fetch-res)
		         nil)))))

(defn handle-error-response [v]
  [:fail v])

(defn handle-read-response [work-unit v]
  (let [[resp-vec error-vec] (read-fetch-message work-unit v)]
    [:ok resp-vec]))

(defn handle-timeout-response []
  [:fail nil])

(defn handle-response 
  "Listens to a response after a fetch request has been sent
   Returns [status data]  status can be :ok, :timeout, :error and data is v returned from the channel"
  [{:keys [client] :as state} work-unit conf]
  (let [fetch-timeout (get conf :fetch-timeout 10000)
        {:keys [read-ch error-ch]} client
        [v c] (alts!! [read-ch error-ch (timeout fetch-timeout)])]
    (condp = c
      read-ch (cond 
                (instance? Reconnected v) (handle-response state conf)
                (instance? Poison v) [:fail nil]
                :else (handle-read-response work-unit v))
      error-ch (handle-error-response v)
      (handle-timeout-response))))
     
    
(defn fetch-and-wait 
  "
   Sends a request for the topic and partition to the producer
   Returns [status data]  status can be :ok, :fail and data is v returned from the channel"
  [state {:keys [topic partition offset len] :as work-unit} producer]
    (send-fetch producer [[topic [{:partition partition :offset offset}]]])
    (handle-response producer work-unit (get state :conf)))


(defn wait-on-work-unit!
  "Blocks on the redis queue till an item becomes availabe, at the same time the item is pushed to the working queue"
  [redis-conn queue working-queue]
  (car/wcar redis-conn 
    (car/brpoplpush queue working-queue 0)))

(defn consumer-start [{:keys [redis-conf] :as state}]
  {:pre [(and 
           (:work-queue state) (:working-queue state) (:complete-queue state)
           (:redis-conf state) (:conf state))]}
  (merge state
    {:redis-conn (merge redis-conf {:host (get redis-conf :redis-host (get redis-conf :host))}) 
    :producers {}
    :status :ok}))

(defn consumer-stop [{:keys [producers work-queue working-queue] :as state}] (assoc state :status :ok))

(defn create-producer-if-needed! [producers producer conf]
  (if-let [producer-conn (get producers producer)] 
    [producer-conn producers]
    (let [producer-conn  (create-fetch-producer producer conf)]
      [producer-conn (assoc producers producer producer-conn)])))

(defn publish-work-response! 
  "Remove data from the working-queue and publish to the complete-queue"
  [{:keys [redis-conn working-queue complete-queue]} work-unit status resp-data]
  (car/wcar redis-conn
    (car/lpush complete-queue (assoc work-unit :status status :resp-data resp-data))
    (car/lrem working-queue -1 work-unit)))

(defn save-call [f state & args]
  (try
    (apply f state args)
    (catch Exception t (do (error t t) (assoc state :status :fail)))))

(defn get-work-unit! 
  "Wait for work to become available in the work queue"
  [{:keys [redis-conn work-queue working-queue]}]
  {:pre [redis-conn work-queue working-queue]}
  (wait-on-work-unit! redis-conn work-queue working-queue))


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
  (try
	  (let [{:keys [producer topic partition offset len]} work-unit
	        [producer-conn producers2] (create-producer-if-needed! producers producer conf)]
     (try 
       (do
        (if (not producer-conn) (throw (RuntimeException. "No producer created")))
        (let [[status resp-data] (fetch-and-wait state work-unit producer-conn)
              state2 (merge state (save-call f-delegate state status resp-data))
              ]
          (publish-work-response! state2 work-unit (:status state2) {:offset-read (apply max (map :offset resp-data))})
          (assoc 
            state2
            :producers producers2)))
        (catch Throwable t (do 
                             (publish-work-response! state work-unit :fail nil)
                             (assoc state :status :fail :throwable t :producers  producers2)))))
   (catch Throwable t (assoc state :status :fail :throwable t))))
    
(defn wait-and-do-work-unit! 
  "Combine waiting for a workunit and performing it in one function
   The state as returned by do-work-unit! is returned"
  [state f-delegate]
  (let [work-unit (get-work-unit! state)]
    (do-work-unit! state work-unit f-delegate)))
    
(defn publish-work 
  "Publish a work-unit to the working queue for a consumer connection"
  [{:keys [redis-conn work-queue]} work-unit]
  {:pre [(and (:producer work-unit) (:topic work-unit) (:partition work-unit) (:offset work-unit) (:len work-unit)
           (let [{:keys [host port]} (:producer work-unit)] (and host port)))]}
  (car/wcar redis-conn 
    (car/lpush work-queue work-unit)))


(comment 
  
(use 'kafka-clj.consumer.consumer :reload)
(def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
(def res (do-work-unit! consumer (fn [state status resp-data] state)))

)
    

