(ns kafka-events-disk.core-test
  (:import (java.util.zip GZIPInputStream)
           (java.io InputStreamReader BufferedReader))
  (:require [kafka-events-disk.core :refer :all]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [midje.sweet :refer :all]))


(defn- file->line-seq
  "Returns a line seq from a GzipInputStream file"
  [file]
  (->
    file
    io/input-stream
    (GZIPInputStream.)
    (InputStreamReader.)
    (BufferedReader.)
    line-seq))

(defn- count-records
  "Count the records "
  [path]
  (->> path
       io/file
       file-seq
       rest                                                 ;skip first element which is the directory
       (map file->line-seq)
       flatten
       count))

(facts "Test writing data to"
       (fact "Test registering and writing events to file"
             (let [ ch (async/chan 2000)
                    node {:work-unit-event-ch ch}
                    path (str "target/test-" (System/currentTimeMillis))]
               (register-writer! node {:path path})
               ;check that the ape connection is registered with writers
               (get @writers ch) => truthy

               (dotimes [i 1000]
                 (async/>!! ch {:msg "abc"}))

               (close-writer! node)
               (get @writers ch) => falsey
               (Thread/sleep 10000)
               (count-records path) => 1000
               )
            ))
