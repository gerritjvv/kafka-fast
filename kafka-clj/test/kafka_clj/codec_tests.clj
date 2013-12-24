(ns kafka-clj.codec-tests
  (:require [kafka-clj.codec :refer :all])
  (:use midje.sweet))

(facts "Test codecs"
       
       (fact "Test crc32"
             (crc32-int (.getBytes "hi")) => (complement nil?) )
       (fact "Get codec ints"
             (get-codec-int "snappy") => 2
             (get-codec-int "gzip") => 1
             (get-codec-int "none") => 0
             (get-codec-int "other") => (throws Exception) ))
