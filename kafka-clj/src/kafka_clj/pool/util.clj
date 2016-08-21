(ns
  ^{:doc "Pool utility functions and data types"}
  kafka-clj.pool.util
  (:require [clojure.tools.logging :refer [error]]
            [kafka-clj.pool.api :refer :all])
  (:import (java.util.concurrent TimeoutException ThreadFactory Executors ScheduledExecutorService TimeUnit)
           (kafka_clj.pool.api PoolObj)))

;;used to store and object in the pool and determine its idle time
;;ts means the time when the obj was added to the pool

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; util functions

(defn now ^long []
  (System/currentTimeMillis))

(defn call-safely [f & args]
  (try
    (apply f args)
    (catch Exception e (error e e))))

(defn timeout?
  "True if the diff between now and start-ts is more than timeout-ms"
  [timeout-ms start-ts]
  (> (- (now) (long start-ts)) (long timeout-ms)))

(defn assert-timeout!
  "When timeout-ms have passed since start-ts a TimeoutException is thrown"
  [timeout-ms start-ts]
  (when (timeout? timeout-ms start-ts)
    (throw (TimeoutException.))))

(defn ^PoolObj create-pool-obj [v]
  (PoolObj. (now) v))

(defn pool-obj-idle-timeout?
  "True if (diff curr-time (.ts v)) > timeout-ms"
  [^PoolObj v timeout-ms]
  (timeout? timeout-ms (.ts v)))

(defn scheduled-exec-service
  "Create a single threaded exec service that creates daemon threads with name n"
  [n]
  (let [counter (atom 0)]
    (Executors/newScheduledThreadPool (int 1)
                                      (reify ThreadFactory
                                        (newThread [_ r]
                                          (doto
                                            (Thread. ^Runnable r)
                                            (.setName (str n "-" (swap! counter inc)))
                                            (.setDaemon true)))))))

(defn close-scheduled-exec-service!
  "Close and shutdown the executor service"
  [^ScheduledExecutorService exec-service]
  (.shutdown exec-service)
  (.awaitTermination exec-service 10 TimeUnit/SECONDS)
  (.shutdownNow exec-service))

(defn schedule
  "Scheduled the function f at a fixed rate at check-freq-ms"
  [^ScheduledExecutorService exec-service check-freq-ms ^Runnable f]
  (.scheduleWithFixedDelay exec-service
                           f
                           (long check-freq-ms)
                           (long check-freq-ms)
                           TimeUnit/MILLISECONDS))