(ns kafka-clj.client-tests
  (:require [kafka-clj.client :as client]
            [kafka-clj.msg-persist :as persist]
            [kafka-clj.response :as response])
  (:use midje.sweet
        conjure.core))


(facts "Test client utility functions"
       (fact "Test select-partition-rc"
             (let [state
                   {:brokers-metadata-ref (ref {:a [{:host :a :port 1 :error-code 0}
                                                    {:host :b :port 2 :error-code 0}]})
                    :blacklisted-producers-ref (ref {})}]

               (client/select-partition-rc state :a) => #(:host %)
               (dosync (commute (:blacklisted-producers-ref state) assoc-in [:a 1] true))
               (:host (client/select-partition-rc state :a)) => :b
               (dosync (commute (:blacklisted-producers-ref state) assoc-in [:b 2] true))
               (client/select-partition-rc state :a) => nil)))



(facts "Test kafka-response with different error-code settings, also test nil cases"
       (let [persist-called (atom 0)
             handle-async-called (atom 0)]
         ;get-sent-message state topic partition correlation-id
         (stubbing [persist/get-sent-message (fn [_ topic partition correlation-id] (swap! persist-called inc)
                                               topic => "topic"
                                               partition => 1
                                               correlation-id => 1)
                    client/handle-async-topic-messages (fn [_ _ _] (swap! handle-async-called inc))]

                   (let [error-code 1]
                     (client/kafka-response {:send-cache true} (response/->ProduceResponse 1 "topic" 1 error-code nil))
                     @persist-called => 1
                     @handle-async-called => 1)

                   (let [error-code 0]
                     (client/kafka-response {:send-cache true} (response/->ProduceResponse 1 "topic" 1 error-code nil))
                     ;;the calls to persist-called and handle-async-called should be at 1 because they should not be
                     ;;called again because we have error-code 0
                     @persist-called => 1
                     @handle-async-called => 1)

                   ;; Test error-code nil values
                   (let [error-code nil]
                     (client/kafka-response {:send-cache true} (response/->ProduceResponse 1 "topic" 1 error-code nil))
                     ;;the calls to persist-called and handle-async-called should be at 1 because they should not be
                     ;;called again because we have error-code 0
                     @persist-called => 1
                     @handle-async-called => 1))))