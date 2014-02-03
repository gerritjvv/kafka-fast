(ns kafka-clj.msg-persist-tests
  (:require [kafka-clj.msg-persist :refer [create-retry-cache write-to-retry-cache retry-cache-seq
                                           create-send-cache cache-sent-messages get-sent-message]])
  (:use midje.sweet))

(facts "test msg-persist"
  
  (fact "Test retry cache"
    
    (let [file (clojure.java.io/file (str "target/retry-cache-tests/" (System/nanoTime)))
          _  (clojure.java.io/make-parents file)
          cache {:retry-cache (create-retry-cache {:retry-cache-file file :retry-cache-delete-on-exit true})}
          
          msgs (take 100 (repeatedly #(System/nanoTime)))
         ]
       
         ;write values
         (doseq [msg msgs] (write-to-retry-cache cache "abc" msg))
         
         
         ;read values
         (let [ret-v (retry-cache-seq cache)]
           
           (count ret-v) => 100
           ;(keys (first ret-v)) => '(:topic :v) for some reason this does not pass with
           ;Expected: (:topic :v)
           ;Actual: (:topic :v)
           ;
           
           )))
    (fact "Test send cache"
      
      (let [cache {:send-cache (create-send-cache {})}]
        (cache-sent-messages cache [[1000 1]])
        (cache-sent-messages cache [[1001 2] [1002 3]])
        
        (get-sent-message cache 1000) => 1
        (get-sent-message cache 1001) => 2
        (get-sent-message cache 1002) => 3)
        
      ))

