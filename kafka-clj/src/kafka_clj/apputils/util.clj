(ns kafka-clj.apputils.util)

(defn format-brokers [brokers]
  (reduce #(conj %1 {:host %2 :port 9092}) [] brokers))
