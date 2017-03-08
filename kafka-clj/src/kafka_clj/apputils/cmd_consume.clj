(ns
  ^{:doc "Consume command "}
  kafka-clj.apputils.cmd-consume

  (:require [kafka-clj.apputils.util :as app-util]
            [kafka-clj.consumer.node :as node]))

(comment
  (def)
  (def node (create-node! consumer-conf ["ping"]))

  (read-msg! node))


(defn consume [topic brokers redis-host]
  (let [brokers' (app-util/format-brokers (clojure.string/split brokers #"[,;]"))

        consumer-conf {:bootstrap-brokers brokers' :redis-conf {:host redis-host :max-active 5 :timeout 1000 :group-name (str "test-" (System/currentTimeMillis))} :conf {:use-earliest true :consumer-reporting true}}
        connector (node/create-node! consumer-conf [topic])

        k 100000

        last-seen-ts (atom (System/currentTimeMillis))
        counter (atom 0)]

    (while (node/read-msg! connector)
      (let [i (swap! counter inc)]
        (when (zero? (rem i k))
          (let [ts (- (System/currentTimeMillis) @last-seen-ts)]
            (swap! last-seen-ts (constantly (System/currentTimeMillis)))
            (println (.getName (Thread/currentThread))
                     "Read " i " messages " ts "ms Rate " (int (/ k (/ ts 1000))) "p/s")))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; public

(defn consume-data [topic brokers redis-host]
  (consume topic brokers redis-host))