(ns kafka-clj.consumer.node
  (:require

            [kafka-clj.consumer.work-organiser :refer [create-organiser! close-organiser! calculate-new-work]]
            [kafka-clj.consumer.consumer :refer [consume! close-consumer!]]
            [group-redis.core :as gr]
            [fun-utils.core :refer [fixdelay stop-fixdelay]]
            [taoensso.carmine.locks :as locks]

            [clojure.tools.logging :refer [info error]]
            [clojure.core.async :refer [chan <!! alts!! timeout close! sliding-buffer]]))



;; Represents a single consumer node that has a
;; redis-group-conn and registers as a member
;; organiser only if the master
;; consumer

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
  (safe-call gr/close group-conn)
  (safe-call close! msg-ch))

(defn- work-calculate-delegate!
  "Checks the reentrant-lock $group-name/\"kafka-nodes-master-lock\" and if it returns true
   the (calculate-new-work node topics) function is called"
  [{:keys [redis-conn group-name] :as node} topics]
  {:pre [redis-conn topics group-name]}
  ;timeout-ms wait-ms
  (locks/with-lock
    redis-conn
    (str group-name "/kafka-nodes-master-lock")
    60000
    500
    (let [ts (System/currentTimeMillis)
          _ (calculate-new-work node topics)
          total-time (- (System/currentTimeMillis) ts)]
      ;(info "Calculating new work for " (count topics) " took " total-time " ms")
      )))

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

(defn create-node!
  "Create a consumer node that represents a consumer using an organiser consumer and group conn to coordinate colaborative consumption
  The following keys must exist in the configuration
  :redis-conf {:group-name :host } defaults group-name \"default\" host localhost
  :bootstrap-brokers e.g [{:host :port}]


  Redis groups:
   Three redis groups are created, $group-name-\"kafka-work-queue\", $group-name-\"kafka-working-queue\", $group-name-\"kafka-complete-queue\"

  Returns a map {:conf intermediate-conf :topics-ref topics-ref :org org :msg-ch msg-ch :consumer consumer :calc-work-thread calc-work-thread :group-conn group-conn :group-name group-name}
  "
  [conf topics]
  {:pre [conf topics]}
  (let [topics-ref (ref (into #{} topics))
        group-name (get-in conf [:redis-conf :group-name] "default")
        intermediate-conf (assoc conf :work-queue (str group-name "-kafka-work-queue")
                                      :working-queue (str group-name "-kafka-working-queue")
                                      :complete-queue (str group-name "-kafka-complete-queue"))

        work-unit-event-ch (chan (sliding-buffer 100))
        org (create-organiser! intermediate-conf)
        group-conn (:group-conn org)
        msg-ch (chan 1000)
        consumer (consume! (assoc intermediate-conf :msg-ch msg-ch :work-unit-event-ch work-unit-event-ch))
        calc-work-thread (start-work-calculate (assoc org :group-name group-name :redis-conn (:conn group-conn)) topics-ref)
        ]

    (gr/join group-conn)

    {:conf intermediate-conf :topics-ref topics-ref :org org :msg-ch msg-ch :consumer consumer :calc-work-thread calc-work-thread
     :group-conn group-conn :group-name group-name
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