(ns kafka-clj.consumer.node
  (:import [java.net InetAddress])
  (:require
            [kafka-clj.consumer.work-organiser :refer [create-organiser! close-organiser! calculate-new-work]]
            [kafka-clj.consumer.consumer :refer [consume! close-consumer!]]
            [kafka-clj.redis :as redis]
            [fun-utils.core :refer [fixdelay stop-fixdelay]]
            [taoensso.carmine :as car]
            [clojure.tools.logging :refer [info error]]
            [clojure.core.async :refer [chan <!! alts!! timeout close! sliding-buffer]]))


(defn- safe-call
  "Util function that calls f with args and if any exception prints the error"
  [f & args]
  (try
    (apply f args)
    (catch Exception e (error e e))))

(defn shutdown-node!
  "Closes the consumer node"
  [{:keys [group-conn org consumer msg-ch calc-work-thread] :as node}]
  {:pre [group-conn org consumer msg-ch calc-work-thread]}
  (stop-fixdelay calc-work-thread)
  (safe-call close-consumer! consumer)
  (safe-call close-organiser! org)
  (safe-call close! msg-ch))

(defn- work-calculate-delegate!
  "Checks the reentrant-lock $group-name/\"kafka-nodes-master-lock\" and if it returns true
   the (calculate-new-work node topics) function is called"
  [{:keys [redis-conn group-name] :as node} topics]
  {:pre [redis-conn topics group-name]}
  ;timeout-ms wait-ms
  (redis/with-lock
    redis-conn
    (str group-name "/kafka-nodes-master-lock")
    (* 5 60000)                                             ;5 minute timeout
    500
    (calculate-new-work node topics)))

(defn- start-work-calculate
  "Returns a channel that will run in a fixed delay of 1000ms
   and call calculate-new-work from the (deref topics) only if the current node is the master

   Note this means that all nodes need to have the same reference to the same topics over all the consumer machines.
   One way of doing this is treating all consumers as masters but only one of them will make the actual work assignment.
   The topics can be polled from a configuration service like zookeeper or even a DB"
  [org topics]
  {:pre [org topics]}
  (fixdelay 10000
            (safe-call work-calculate-delegate! org @topics)))

(defn copy-redis-queue
  "This function copies data from one list/queue to another
   Its used on startup to copy any leftover workunits in the working queue to the work queue"
  [redis-conn from-queue to-queue]
  (if (= from-queue to-queue)
    (throw (RuntimeException. "Cannot copy to and from the same queue")))

  (loop [len (redis/wcar redis-conn (car/llen from-queue))]
    (info "copy-redis-queue [" from-queue "] => [" to-queue "]: " len)
    (if (> len 0)
      (let [wus (redis/wcar redis-conn
                            (car/lrange from-queue 0 100))]
        (when (not-empty wus)
          (let [res
                (redis/wcar redis-conn
                            (doall (map #(car/lrem from-queue -1 %) wus))
                            (apply car/lpush to-queue wus))]

            ;drop-last to drop the result from teh apply car lpush command
            (if (>= (apply + (drop-last res)) (count wus))                            ;only recur if we did delete all the values
              (recur (redis/wcar redis-conn (car/llen from-queue))))))))))

(defn create-node!
  "Create a consumer node that represents a consumer using an organiser consumer and group conn to coordinate colaborative consumption
  The following keys must exist in the configuration
  :redis-conf {:group-name :host } defaults group-name \"default\" host localhost
  :bootstrap-brokers e.g [{:host :port}]


  Redis groups:
   Three redis groups are created, $group-name-\"kafka-work-queue\", $group-name-\"kafka-working-queue\", $group-name-\"kafka-complete-queue\", $group-name-\"kafka-error-queue\"

  Note that unrecouverable work units like error-code 1 are added to the kafka-error-queue. This queue should be monitored.
  Returns a map {:conf intermediate-conf :topics-ref topics-ref :org org :msg-ch msg-ch :consumer consumer :calc-work-thread calc-work-thread :group-conn group-conn :group-name group-name}
  "
  [conf topics]
  {:pre [conf topics]}
  (let [host-name (.getHostName (InetAddress/getLocalHost))
        topics-ref (ref (into #{} topics))
        group-name (get-in conf [:redis-conf :group-name] "default")
        working-queue-name (str group-name "-kafka-working-queue/" host-name)
        work-queue-name (str group-name "-kafka-work-queue")
        intermediate-conf (assoc conf :group-name group-name
                                      :work-queue work-queue-name
                                      :working-queue working-queue-name
                                      :error-queue (str group-name "-kafka-erorr-queue")
                                      :complete-queue (str group-name "-kafka-complete-queue"))

        work-unit-event-ch (chan (sliding-buffer 100))
        org (create-organiser! intermediate-conf)
        redis-conn (:redis-conn org)
        msg-ch (chan 1000)
        consumer (consume! (assoc intermediate-conf :redis-conn redis-conn :msg-ch msg-ch :work-unit-event-ch work-unit-event-ch))
        calc-work-thread (start-work-calculate (assoc org :redis-conn redis-conn) topics-ref)

        working-len  (redis/wcar redis-conn
                                 (car/llen working-queue-name))
        ]


    ;check for left work-units in working queue
    (copy-redis-queue redis-conn working-queue-name work-queue-name)

    {:conf intermediate-conf :topics-ref topics-ref :org org :msg-ch msg-ch :consumer consumer :calc-work-thread calc-work-thread
      :group-name group-name
     :work-unit-event-ch work-unit-event-ch}))

(defn add-topics!
  "Add topics to the node's topics-ref set, this will cause the orgnaniser run by the node to check for workunits for the topics
   topics must be a vector seq or set"
  [node topics]
  {:pre [node (:topics-ref node) topics (coll? topics)]}
  (dosync
    (alter (:topics-ref node) into topics)))

(defn remove-topics!
  "Removes topics from the node's topics-ref set, this will cause the organiser to stop submitting workunits for the topics
   topics must be a vector seq or set"
  [node topics]
  {:pre [node (:topics-ref node) topics (coll? topics)]}
  (dosync
    (alter (:topics-ref node) clojure.set/difference (set topics))))

(defn read-msg!
  "Accepts a the return value of create-node! and blocks on msg-ch
   The return value is a collection of Message [topic partition offset bts]"
  ([{:keys [msg-ch]} timeout-ms]
   (alts!! [msg-ch (timeout timeout-ms)]))
  ([{:keys [msg-ch]}]
   (<!! msg-ch)))

(defn- add-msg-seq-state [state {:keys [topic partition offset]}]
  (assoc! state (str topic partition) offset))

(defn msg-seq!
  ([node]
   (cons (read-msg! node)
         (lazy-seq (msg-seq! node)))))

(defn msg-seq-buffered!
  "Will always return a sequence of sequence of messages i.e [ [msg1, msg2, msg3] .. ]
   Acceps :step n which is the number of messages per sequence inside the main sequence"
  [node & {:keys [step] :or {step 1000}}]
  (partition-all step (msg-seq! node)))

(comment

  (use 'kafka-clj.consumer.node :reload)
  (require '[clojure.core.async :as async])
  (def consumer-conf {:bootstrap-brokers [{:host "localhost" :port 9092}] :redis-conf {:host "localhost" :max-active 5 :timeout 1000 :group-name "test"} :conf {}})
  (def node (create-node! consumer-conf ["ping"]))

  (read-msg! node)
  ;;for a single message
  (def m (msg-seq! node))
  ;;for a lazy sequence of messages

  )