(ns kafka-clj.auto-tune-fetch-tests
  (:require
    [kafka-clj.consumer.consumer :as consumer]
    [conjure.core :refer :all])
  (:use midje.sweet))


(facts "Test auto tune decrements when messages discarded > 5
        For this test we start with a ma-bytes-at test/partition = 400 000
        We expect a subtract to occur of init-max-bytes - 1450
        "
       (let [offset 100
             maxoffset 100
             discarded 10
             minbts 123
             maxbts 456

             topic "test"
             total-processed 10

             init-max-bytes [7340032 524288 524288 (System/currentTimeMillis)]
             max-bytes-at (atom {topic {0 init-max-bytes}})]


         (stubbing
           [kafka-clj.consumer.consumer/process-wu! (fn [_ _ _ _]
                                                      [_ offset maxoffset discarded minbts maxbts total-processed])]

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic :partition 0})

           ;;check that the actual subtraction did happen
           (drop-last (get-in @max-bytes-at [topic 0])) =>  [6815744 524288 655360])))

(facts "Test auto tune increments when maxoffset-offset > 5, this means that we have not consumed all the messages for this wu
        and the max bytes needs to be increased.
        We test for an increase of init-max-bytes + 290"
       (let [offset 100
             maxoffset 400
             discarded 0
             minbts 123
             maxbts 456
             init-max-bytes [7340032 524288 524288 (System/currentTimeMillis)]
             topic "test"
             total-processed 10

             max-bytes-at (atom {topic {0 init-max-bytes}})]


         (stubbing
           [kafka-clj.consumer.consumer/process-wu! (fn [_ _ _ _]
                                                      [_ offset maxoffset discarded minbts maxbts total-processed])]

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic :partition 0})

           ;;check that the addition did happen
           (drop-last (get-in @max-bytes-at [topic 0])) => [7864320 786432 524288])))

(facts "Test auto tune handles nil returned"
       (let [topic "test"
             max-bytes-at (atom {})]


         (stubbing
           [kafka-clj.consumer.consumer/process-wu! (fn [& args]
                                                      nil)]

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic :partition 0})

           (get-in @max-bytes-at [topic 0]) => nil)))

(facts "Test that the auto-tune hanles empty max-byte-at atoms, in our test we duplicate the test for
        increment with the only difference being using an empty map."
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

           (consumer/auto-tune-fetch max-bytes-at {} {} (fn []) {:topic topic :partition 0})

           ;;check that the addition did happen
           (drop-last (get-in @max-bytes-at [topic 0])) => [7340032 524288 524288])))
