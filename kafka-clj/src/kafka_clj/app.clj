(ns kafka-clj.app
  (:require [kafka-clj.apputils.cmd-produce :as cmd-produce]
            [kafka-clj.apputils.cmd-consume :as cmd-consume]
            [kafka-clj.apputils.cmd-work-queue :as cmd-work-queue]

            )
  (:gen-class))


(defn print-help []
  (println "
  cmd <send|consume>

  send:  send test json data to kafka
         send <broker-list> <threads> <num-of-records-per-thread>

  consume: consume data from topics in kafka on a unique group and from the beginning
           used for performance and stability testing
           will run till ctrl+c or kill <pid>
           consume <broker-list> redis-host <topics>

  work-queue: display work units in the work queue
           host from limit [password <redis-password>]

  "))

(defn consume [brokers redis-host topic]
  (cmd-consume/consume-data topic brokers redis-host))

(defn send-msgs [brokers topic threads count-per-thread & conf]
  (cmd-produce/send-data threads count-per-thread topic brokers (apply array-map conf)))

(defn work-queue [& args]
  (cmd-work-queue/read-redis-queue args))

(defn -main [& args]
  (try
    (let [[cmd & opts] args]

      (case cmd
        "send" (apply send-msgs opts)
        "consume" (apply consume opts)
        "work-queue" (apply work-queue opts)
        (print-help)))
    (finally
      (System/exit 0))))