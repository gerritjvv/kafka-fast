(ns
  ^{:author "gerritjvv"
    :doc "Internal consumer helper api for receiving and publishing work units to redis
         The public functions are get-work-unit!, publish-consumed-wu!, publish-error-wu! and publisher-zero-consumed-wu!"}
  kafka-clj.consumer.workunits
  (:require [clojure.tools.logging :refer [info debug]]
            [kafka-clj.redis.core :as redis])
  (:import [java.net SocketTimeoutException]
           (java.util.concurrent.atomic AtomicBoolean)))

(defn- safe-sleep
  "Util function that does not print an Interrupted exception but handles by setting the current thread to interrupt"
  [ms]
  (try
    (Thread/sleep ms)
    (catch InterruptedException i (doto (Thread/currentThread) (.interrupt)))))

(defn- publish-work-response!
  "Remove data from the working-queue and publish to the complete-queue"
  [{:keys [redis-conn working-queue complete-queue work-queue work-unit-event-ch] :as state} org-work-units work-unit status resp-data]
  {:pre [work-unit-event-ch redis-conn working-queue complete-queue work-queue work-unit]}
  (let [sorted-wu (into (sorted-map) work-unit)
        work-unit2 (assoc sorted-wu :status status :resp-data resp-data)]

    ;send work complete to complete-queue
    (redis/wcar redis-conn
                (redis/lpush redis-conn complete-queue work-unit2)
                (redis/lrem  redis-conn working-queue -1 sorted-wu))
    state))

(defn publish-consumed-wu!
  "sends the wu as ok and the offset read"
  [state wu offset-read]
  (publish-work-response! state wu wu :ok {:offset-read offset-read}))

(defn publish-error-wu!
  "sends the wu as error status"
  [state wu status offset-read]
  (publish-work-response! state wu wu status {:offset-read offset-read}))

(defn publish-zero-consumed-wu! [state wu]
  (publish-work-response! state wu wu :ok {:offset-read 0}))

(defn publish-error-consumed-wu! [state wu]
  (publish-work-response! state wu wu :fail {:offset-read 0}))

(defn wait-on-work-unit!
  "Blocks on the redis queue till an item becomes availabe, at the same time the item is pushed to the working queue"
  [redis-conn queue working-queue shutdown-flag]
  {:pre [redis-conn queue working-queue shutdown-flag]}
  (if-let [res (try                                         ;this command throws a SocketTimeoutException if the queue does not exist
                 (redis/wcar redis-conn                       ;we check for this condition and continue to block
                             (redis/brpoplpush redis-conn queue working-queue 1000))
                 (catch SocketTimeoutException e (do (safe-sleep 1000) (debug "Timeout on queue " queue " retry ") nil)))]
    res
    (when-not (.get ^AtomicBoolean shutdown-flag)
      (recur redis-conn queue working-queue shutdown-flag))))


(defn get-work-unit!
  "Wait for work to become available in the work queue
   Adds a :seen key to the work unit with the current milliseconds"
  [{:keys [redis-conn work-queue working-queue shutdown-flag]}]
  {:pre [redis-conn work-queue working-queue]}
  (let [ts (System/currentTimeMillis)
        wu (wait-on-work-unit! redis-conn work-queue working-queue shutdown-flag)
        diff (- (System/currentTimeMillis) ts)]
    (if (> diff 1000)
      (info "Slow wait on work unit: took: " diff "ms"))
    wu))