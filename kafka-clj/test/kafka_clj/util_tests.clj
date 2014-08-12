(ns kafka-clj.util-tests
  (:require [kafka-clj.client :refer :all])
  (:use midje.sweet))


(facts "Test util functions"
  (fact "Test find-hashed-connection"
    (let [producers {"a:1" 1 "a:2" 2}]
      (find-hashed-connection "a:1" producers 6) => 1
      (find-hashed-connection "b:1" producers 6) => 2
      (find-hashed-connection "c:100" producers 6) => nil)))
      
      
      