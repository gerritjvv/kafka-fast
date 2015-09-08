(ns kafka-clj.auto-tune-fetch-tests
  (:require
    [kafka-clj.consumer.consumer :as consumer]
    [conjure.core :refer :all])
  (:use midje.sweet))


(facts "Test auto tune increment"
       (let [offset 100
             maxoffset 100
             discarded 10
             minbts 123
             maxbts 456

             topic "test"
             total-processed 10

             max-bytes-at (atom {})]


         (stubbing
           [kafka-clj.consumer.consumer/process-wu! (fn [_ _ _ _]
                                                      [_ offset maxoffset discarded minbts maxbts total-processed])]

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic})

           (get @max-bytes-at topic) => 145)))


(facts "Test auto tune decrement"
       (let [offset 100
             maxoffset 400
             discarded 0
             minbts 123
             maxbts 456

             topic "test"
             total-processed 10

             max-bytes-at (atom {})]


         (stubbing
           [kafka-clj.consumer.consumer/process-wu! (fn [_ _ _ _]
                                                      [_ offset maxoffset discarded minbts maxbts total-processed])]

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic})

           (get @max-bytes-at topic) => 29)))
