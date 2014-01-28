(ns kafka-clj.metrics
  (require [kafka-clj.consumer :refer [metrics-registry]])
  (import [com.codahale.metrics ConsoleReporter CsvReporter]
          [java.io File]
          [java.util Locale]
          [java.util.concurrent TimeUnit]))


(defn report-consumer-metrics [report-type & {:keys [freq dir] :or {freq 10 dir "/tmp/kafka-consumer-metrics"}}]
  "Launches one of the standard reporters for the consumers, all consumer metrics will be reported on
   report-type can be :console or :csv, the second parameter is a map that contains freq in seconds and dir if the report type is csv"
  (cond 
      (= report-type :console)
      (-> (ConsoleReporter/forRegistry metrics-registry) (.convertRatesTo TimeUnit/SECONDS)
        (.convertDurationsTo TimeUnit/MILLISECONDS)
        .build
        (.start (int freq) TimeUnit/SECONDS))
      (= report-type :csv)
      (do 
        (.mkdirs (File. (str dir)))
	      (-> (CsvReporter/forRegistry metrics-registry) 
	          (.formatFor Locale/US)
	          (.convertRatesTo TimeUnit/SECONDS)
	          (.convertDurationsTo TimeUnit/MILLISECONDS)
	          (.build (File. (str dir)))
	          (.start (int freq) TimeUnit/SECONDS)))
      :else 
         (throw (RuntimeException. (str "The report type " report-type " is not supported")))))

