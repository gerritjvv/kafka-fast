(ns kafka-clj.debug)

(defn write-debug [n msg]
  (spit (str "/tmp/writedebug-" n) (str (:uuid msg) "," (:len msg) "," (:status msg) "\n") :append true))


(defonce l 1)
(defn write-trace [msg status]
  (locking l
    (spit "/tmp/write-trace" (str (:uuid msg) "," (:len msg) "," status "\n") :append true)))