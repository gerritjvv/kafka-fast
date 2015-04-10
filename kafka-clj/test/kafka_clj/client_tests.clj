(ns kafka-clj.client-tests
  (:require [kafka-clj.client :as client])
  (:use midje.sweet))


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
