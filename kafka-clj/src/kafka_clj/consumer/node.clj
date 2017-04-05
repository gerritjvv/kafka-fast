(ns kafka-clj.consumer.node
  (:import [java.net InetAddress]
           (org.openjdk.jol.info GraphLayout))
  (:require
    [kafka-clj.consumer.work-organiser :refer [create-organiser! close-organiser! calculate-new-work]]
    [kafka-clj.consumer.consumer :refer [consume! close-consumer! consumer-pool-stats work-unit-ack!]]
    [kafka-clj.redis.core :as redis]
    [com.stuartsierra.component :as component]
    [fun-utils.core :refer [fixdelay-thread stop-fixdelay buffered-chan]]
    [clojure.tools.logging :refer [info error]]
    [clojure.core.async :refer [chan <!! alts!! timeout close! sliding-buffer]]))


(defn- safe-call
  "Util function that calls f with args and if any exception prints the error"
  [f & args]
  (try
    (apply f args)
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)))))

(defn shutdown-node!
  "Closes the consumer node"
  [{:keys [org consumer msg-ch calc-work-thread redis-conn] :as node}]
  {:pre [org consumer msg-ch calc-work-thread redis-conn]}

  (stop-fixdelay calc-work-thread)
  (safe-call close-consumer! consumer)
  (safe-call close-organiser! org :close-redis false)
  (safe-call redis/close! redis-conn)
  (safe-call close! msg-ch))

(defn- work-calculate-delegate!
  "Checks the reentrant-lock $group-name/\"kafka-nodes-master-lock\" and if it returns true
   the (calculate-new-work node topics) function is called"
  [{:keys [redis-conn group-name] :as node} topics]
  {:pre [redis-conn topics group-name]}
  ;timeout-ms wait-ms
  (let [lock-timeout (* 10 60000)]
    (try
      (redis/with-lock
        redis-conn
        (str group-name "/kafka-nodes-master-lock")
        lock-timeout
        1000
        (calculate-new-work node topics))
      (catch Exception e (error e e)))))

(defn- start-work-calculate
  "Returns a channel that will run in a fixed delay of 1000ms
   and call calculate-new-work from the (deref topics) only if the current node is the master

   Note this means that all nodes need to have the same reference to the same topics over all the consumer machines.
   One way of doing this is treating all consumers as masters but only one of them will make the actual work assignment.
   The topics can be polled from a configuration service like zookeeper or even a DB"
  [org topics & {:keys [freq] :or {freq 10000}}]
  {:pre [org topics]}

  (fixdelay-thread
    freq
    (safe-call work-calculate-delegate! org @topics)))

(defn copy-redis-queue
  "This function copies data from one list/queue to another
   Its used on startup to copy any leftover workunits in the working queue to the work queue"
  [redis-conn from-queue to-queue]
  (if (= from-queue to-queue)
    (throw (RuntimeException. "Cannot copy to and from the same queue")))



  (loop [len (redis/wcar redis-conn (redis/llen redis-conn from-queue))]
    (info "copy-redis-queue [" from-queue "] => [" to-queue "]: " len)
    (when (pos? len)
      (redis/wcar redis-conn
                  (redis/lua redis-conn
                             (str
                               "local s = '" from-queue "'"
                               "local d = '" to-queue "'"
                               "local i = tonumber(redis.call(\"LLEN\", s))
                               local j = 0
                               while j < i do
                                local l = redis.call(\"LRANGE\", s, j, j+99)
                                redis.call(\"LPUSH\", d, unpack(l))
                                j = j + 100
                               end
                               redis.call(\"DEL\", s)"))))))

(defn create-node!
  "Create a consumer node that represents a consumer using an organiser consumer and group conn to coordinate colaborative consumption
  The following keys must exist in the configuration
  :redis-conf {:group-name :host } defaults group-name \"default\" host localhost
  :bootstrap-brokers e.g [{:host :port}]

  Optional keys:
  :conf {:work-calculate-freq     ;;the frequency in millis at which new work is calculated, default 10000ms
         :use-earliest ;;if the topic information is not saved to redis the earliest available offset is used and saved,
                       ;;otherwise the most recent offset is used.
         :pool-limit 20 ;;the tcp pool limit for the consumer
         :max-offsets ;;default 10, if use-earliest is true the earliest offset is used looking back up to max-offsets
         :jaas if set the jaas authentication will be used with each tcp connection
                this value should point to the jaas config file.
                for more information see http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/AcnOnly.html

         }


  Redis groups:
   Three redis groups are created, $group-name-\"kafka-work-queue\", $group-name-\"kafka-working-queue\", $group-name-\"kafka-complete-queue\", $group-name-\"kafka-error-queue\"

  Note that unrecouverable work units like error-code 1 are added to the kafka-error-queue. This queue should be monitored.
  Returns a map {:conf intermediate-conf :topics-ref topics-ref :org org :msg-ch msg-ch :consumer consumer :calc-work-thread calc-work-thread :group-conn group-conn :group-name group-name}
  "
  [conf topics & {:keys [error-handler
                         redis-factory
                         msg-ch-buff-size
                         work-unit-event-ch-buff-size] :or {error-handler                (fn [& args])
                                                            redis-factory                redis/create
                                                            msg-ch-buff-size             100
                                                            work-unit-event-ch-buff-size 100}}]
  {:pre [conf topics (not-empty (:bootstrap-brokers conf)) (:redis-conf conf) (number? msg-ch-buff-size) (number? work-unit-event-ch-buff-size)]}

  (let [host-name (.getHostName (InetAddress/getLocalHost))
        topics-ref (ref (into #{} topics))
        group-name (get-in conf [:redis-conf :group-name] "default")
        working-queue-prefix (str group-name "-kafka-working-queue/")

        working-queue-name (str working-queue-prefix host-name)
        work-queue-name (str group-name "-kafka-work-queue")
        intermediate-conf (assoc conf
                            :group-name group-name
                            :working-queue-prefix working-queue-prefix
                            :work-queue work-queue-name
                            :working-queue working-queue-name
                            :error-queue (str group-name "-kafka-erorr-queue")
                            :complete-queue (str group-name "-kafka-complete-queue"))

        work-unit-event-ch (chan (sliding-buffer work-unit-event-ch-buff-size))

        ;;contains stats like offset-ahead, see kafka-clj.consumer.work-organiser/calculate-new-work
        stats-atom (atom {})

        org (assoc (create-organiser! intermediate-conf) :error-handler error-handler)
        ;;reuse the redis conn to avoid creating yet another
        redis-conn (:redis-conn org)
        msg-ch (chan msg-ch-buff-size)
        consumer (consume! (assoc intermediate-conf
                             :metadata-connector (:metadata-connector org) ;;re-use the connector pool and managed connections from the organiser
                             :redis-conn redis-conn
                             :msg-ch msg-ch
                             :work-unit-event-ch work-unit-event-ch))
        calc-work-thread (start-work-calculate (assoc org :redis-conn redis-conn
                                                          :stats-atom stats-atom) topics-ref :freq (get conf :work-calculate-freq 10000))
        ]

    ;check for left work-units in working queue
    (copy-redis-queue redis-conn working-queue-name work-queue-name)

    {:conf               intermediate-conf
     :topics-ref topics-ref
     :org org :msg-ch msg-ch
     :consumer consumer
     :stats-atom stats-atom
     :calc-work-thread calc-work-thread
     :group-name         group-name
     :redis-conn         redis-conn
     :work-unit-event-ch work-unit-event-ch}))

(defn node-stats
  "Returns the consumer stats, takes as argument the instance returned from create-node!"
  [{:keys [consumer stats-atom]}]
  (assoc
    @stats-atom :node-stats (consumer-pool-stats consumer)))

(defn conn-pool-idle
  "Return the number of idle redis connections used by the consumer node"
  [{:keys [redis-conn]}]
  (redis/conn-pool-idle redis-conn))

(defn conn-pool-byte-size
  "Return the total bytes referenced by the redis connection pool"
  [{:keys [redis-conn]}]
  (.totalSize (GraphLayout/parseInstance redis-conn)))

(defn conn-pool-active
  "Return the number of active redis connections used by the consumer node"
  [{:keys [redis-conn]}]
  (redis/conn-pool-active redis-conn))

(defn msg-chan-byte-size
  "Return the number of bytes referenced by consumer channel"
  [{:keys [msg-ch]}]
  (.totalSize (GraphLayout/parseInstance msg-ch)))

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
   (let [[msg _] (alts!! [msg-ch (timeout timeout-ms)])]
     msg))
  ([{:keys [msg-ch]}]
   (<!! msg-ch)))


(defn buffered-msgs
  "Creates a channel that on each read returns n messages, or as much as could be buffered befire timeout-ms.
   The buffering happens in the background"
  [{:keys [msg-ch]} n timeout-ms]
  (buffered-chan msg-ch n 1000))

(defn msg-seq!
  ([node]
   (cons (read-msg! node)
         (lazy-seq (msg-seq! node)))))

(defn msg-seq-batches!
  "Partition messages by work unit, effectively creating a batch delivery system, where each bach is N messages long.
   This function is used with consumer/consume! :work-unit-ack => :user and consumer work-unit-ack!
  "
  [node]
  (partition-all :wu (msg-seq! node)))

(defn msg-seq-buffered!
  "Will always return a sequence of sequence of messages i.e [ [msg1, msg2, msg3] .. ]
   Acceps :step n which is the number of messages per sequence inside the main sequence"
  [node & {:keys [step] :or {step 1000}}]
  (partition-all step (msg-seq! node)))

(defn batch-ack!
  "
   Used with msg-seq-batches!
   Acks a single batch of msgs where msgs must all have the same :wu item
   Note that this invariant is not checked but assumed.

   use msg-seq-batches! to consume mssages partitioned by :wu"
  [node msgs]
  (when-let [max-msg (apply max-key :offset msgs)]
    (work-unit-ack! (:consumer node) (:wu max-msg) (:offset max-msg))))

(defrecord KafkaNodeService [conf topics]
  component/Lifecycle
  (start [component]
    (assoc component :node (create-node! (:conf component) (:topics component))))
  (stop [component]
    (shutdown-node! (:node component))))

(defn create-kafka-node-service [conf topics]
  (->KafkaNodeService conf topics))
