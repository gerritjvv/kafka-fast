(ns kafka-clj.consumer-tests
  (:require [kafka-clj.consumer :refer :all])
  (:use midje.sweet))


(facts "test offsets"
  
  (fact "test merge offsets"
    
    (sort-by (fn [m] (vec (vals m))) (replace-partition [{:partition 1 :offset 1} {:partition 2 :offset 0}] 100 2)) => 
    (sort-by (fn [m] (vec (vals m))) [{:partition 1 :offset 1} {:partition 2 :offset 100}])
    
    (let [v {
						 {:host "server07", :port 9092}
						 [{:topic "mylog",
						   :partition 5,
						   :offset 100, :bts nil}
						  {:topic "mylog",
						   :partition 4,
						   :offset 200, :bts nil}]
						
						{:host "server03", :port 9092}
						 [{:topic "mylog",
						   :partition 6,
						   :offset 101, :bts nil}
						  {:topic "mylog",
						   :partition 7,
						   :offset 201, :bts nil}]
						  }
          m {{:host "server07", :port 9092}
						 {"mylog"
						  [{:offset 0, :error-code 0, :locked true, :partition 4}
						   {:offset 0, :error-code 0, :locked true, :partition 5}]}
						
						 {:host "server03", :port 9092}
						 {"mylog"
						  [{:offset 0, :error-code 0, :locked true, :partition 6}
						   {:offset 0, :error-code 0, :locked true, :partition 7}]}}
          r (merge-broker-offsets m v)]
          (clojure.pprint/pprint r)
          r =>  {
                 {:host "server07", :port 9092}
								 {"mylog"
								  [{:offset 200, :error-code 0, :locked true, :partition 4}
								   {:offset 100, :error-code 0, :locked true, :partition 5}]}
								 {:host "server03", :port 9092}
								 {"mylog"
								  [{:offset 201, :error-code 0, :locked true, :partition 7}
								   {:offset 101, :error-code 0, :locked true, :partition 6}]}})
      
    ))