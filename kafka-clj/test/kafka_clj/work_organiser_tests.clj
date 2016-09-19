(ns kafka-clj.work-organiser-tests
  (:require
    [kafka-clj.consumer.work-organiser :as wo]
    [midje.sweet :refer :all]))

(defn test-max-offset []
  (fact "Test _max-offset"
        (wo/_max-offset nil [1 1]) => [1 1]
        (wo/_max-offset [2 1] [1 2]) => [2 2]))



(defn test-check-invalid-offsets! []
  (fact "Test check-invalid-offsets!"
        ;;we send offsets from multiple brokers for the same topic and partitions
        ;;here partition 1 is ok and partition 0 contains in the first broker offsets 50, 10, 0
        ;;and then in the second broker offsets 100, 10, 0
        ;;the saved offset will always return 150, more than what is available in either of the partitions
        ;;the check-invalid-offsets should set the redis-offset atom with read offsets to the maximum
        ;;of the last available offsets from either brokers only and only if the read offset is bigger than
        ;;that of the offsets available in all the brokers.
        ;;So we should end up with redis-offset == {"my-group" {"abc" {0 100}}} which means for topic "abc" partition 0 use 100
        ;;in production the check-invalid-offsets! will use redis to save this value
        (let [group-name "my-group"
              inc-f (fn
                      ([a] a)
                      ([a b] (cond
                               (and a b) (+ a b)
                               a a
                               b b)))
              redis-offset (atom {})
              redis-f (fn [_ group-name topic partition max-offset] (swap! redis-offset update-in [group-name topic partition] inc-f max-offset))
              offsets {{:host "abc1" :port 9092} {"abc" [{:offset 50 :all-offsets '(50 10 0) :partition 0}
                                                         {:offset 200 :all-offsets '(200 100 0) :partition 1}]}
                       {:host "abc2" :port 9092} {"abc" [{:offset 50 :all-offsets '(100 10 0) :partition 0}
                                                         {:offset 200 :all-offsets '(200 100 0) :partition 1}]}}

              saved-offset-f (fn [_ _ partition]
                               (case (int partition)
                                 0 150
                                 1 10))]

          (wo/check-invalid-offsets! {:group-name group-name :stats-atom (atom {})} true offsets :redis-f redis-f :saved-offset-f saved-offset-f)

          @redis-offset => {"my-group" {"abc" {0 100}}})))

(facts
  (test-max-offset)
  (test-check-invalid-offsets!))
