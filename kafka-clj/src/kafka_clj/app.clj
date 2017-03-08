(ns kafka-clj.app
  (:require [kafka-clj.apputils.cmd-produce :as cmd-produce]
            [kafka-clj.apputils.cmd-consume :as cmd-consume]
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

  "))

(defn consume [brokers redis-host topic]
  (cmd-consume/consume-data topic brokers redis-host))

(defn send-msgs [brokers topic threads count-per-thread & conf]
  (cmd-produce/send-data threads count-per-thread topic brokers (apply array-map conf)))

(defn -main [& args]
  (try
    (let [[cmd & opts] args]

      (case cmd
        "send" (apply send-msgs opts)
        "consume" (apply consume opts)
        (print-help)))
    (finally
      (System/exit 0))))