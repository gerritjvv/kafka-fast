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
    [io.netty.buffer Unpooled]))


(def byte_array_class (Class/forName "[B"))
(defn byte-array? [arr] (instance? byte_array_class arr))

(defn read-fetch-message 
  "read-fetch will return the result of fn which is [resp-vec error-vec]"
  [v]
  (if (byte-array? v)
	  (let [
	         fetch-res
	         (read-fetch (Unpooled/wrappedBuffer ^"[B" v) [{} [] 0]
				     (fn [state msg]
	              ;read-fetch will navigate the fetch response calling this function
	              ;on each message found, in turn this function will update redis via p-send
	              ;and send the message to the message channel (via >!! msg-ch msg)
	              (if (coll? state)
			            (let [[resp errors cnt] state]
		               (try
			               (do 
					             (cond
								         (instance? Message msg)
								         (let [k #{(:topic msg) (:partition msg)}]
								                 (tuple (assoc resp k msg) errors (inc cnt)))
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

(defn handle-read-response [v]
  (let [[resp-vec error-vec] (read-fetch-message v)]
    [:ok resp-vec]))

(defn handle-timeout-response []
  [:fail nil])

(defn handle-response 
  "Listens to a response after a fetch request has been sent
   Returns [status data]  status can be :ok, :timeout, :error and data is v returned from the channel"
  [{:keys [client]} conf]
  (let [fetch-timeout (get conf :fetch-timeout 10000)
        {:keys [read-ch error-ch]} client
        _ (do (prn "READ_CH " read-ch " --- error-ch " error-ch " client " client))
        [v c] (alts!! [read-ch error-ch (timeout fetch-timeout)])]
    (condp = c
      read-ch (handle-read-response v)
      error-ch (handle-error-response v)
      (handle-timeout-response))))
     
    
(defn fetch-and-wait 
  "
   Sends a request for the topic and partition to the producer
   Returns [status data]  status can be :ok, :fail and data is v returned from the channel"
  [state {:keys [topic partition offset len]} producer]
    (send-fetch producer [[topic {:partition partition :offset offset}]])
    (handle-response producer (get state :conf)))


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

(defn do-work-unit! 
  "state map keys:
    :redis-conn = redis connection params :host :port ... 
    :producers = (ref {}) contains the current brokers to which fetch requests can be sent, these are created dynamically
    :work-queue = the queue name from which work units will be taken, the data must be a map with keys :producer :topic :partition :offset :len
    :working-queue = when a work item is taken from the work-queue its placed on the working-queue
    :complete-queue = when an item has been processed the result is placed on the complete-queue
    :conf = any configuration that will be passed when creating producers"
  
  [{:keys [redis-conn producers work-queue working-queue complete-queue conf] :as state}]
  (try
	  (let [{:keys [producer topic partition offset len] :as work-unit} (wait-on-work-unit! redis-conn work-queue working-queue)
	        [producer-conn producers2] (create-producer-if-needed! producers producer conf)]
     (try 
       (do
       (if (not producer-conn) (throw (RuntimeException. "No producer created")))
       (let [[status resp-data] (fetch-and-wait state work-unit producer-conn)]
         (publish-work-response! state work-unit status resp-data)
         (assoc state
           :status status
           :producers producers2)))
       (catch Throwable t (assoc state :status :fail :throwable t :producers  producers2))))
   (catch Throwable t (assoc state :status :fail :throwable t))))
    
    
(defn publish-work [{:keys [redis-conn work-queue]} work-unit]
  {:pre [(and (:producer work-unit) (:topic work-unit) (:partition work-unit) (:offset work-unit) (:len work-unit)
           (let [{:keys [host port]} (:producer work-unit)] (and host port)))]}
  (car/wcar redis-conn 
    (car/lpush work-queue work-unit)))
    

