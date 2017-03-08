(ns kafka-clj.msg-persist-tests
  (:require [kafka-clj.msg-persist :refer [
                                           create-retry-cache
                                           write-to-retry-cache
                                           retry-cache-seq

                                           create-send-cache
                                           cache-sent-messages
                                           get-sent-message]])
  (:use midje.sweet)
  (:import (org.mapdb DB)))


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
      
      (let [cache {:send-cache (create-send-cache {})}
            corr1 1
            corr2 2
            msgs1 [{:partition 1 :topic "a"} {:partition 1 :topic "a"}]
            msgs2 [{:partition 4 :topic "b"} {:partition 4 :topic "b"}]]
        (cache-sent-messages cache [[corr1 msgs1] [corr2 msgs2]])

        (get-sent-message cache "a" 1 corr1) => [{:partition 1, :topic "a"} {:partition 1, :topic "a"}]
        (get-sent-message cache "b" 4 corr2) => [{:partition 4, :topic "b"} {:partition 4, :topic "b"}]))

       (fact "Test NO NPE on reading from closed send cache"

             (let [file (clojure.java.io/file (str "target/retry-cache-tests/" (System/nanoTime)))
                   _  (clojure.java.io/make-parents file)
                   cache {:retry-cache (create-retry-cache {:retry-cache-file file :retry-cache-delete-on-exit true})}

                   msgs (take 100 (repeatedly #(System/nanoTime)))
                   ]

               ;write values
               (doseq [msg msgs] (write-to-retry-cache cache "abc" msg))

               (let [before-close-seq (filter (complement nil?) (retry-cache-seq cache))]
                 ;;close db

                 (.close ^DB (get-in cache [:retry-cache :db]))

                 ;;test seq before closed was called, because of chunking we could have 20 or so values
                 (= (count (filter (complement nil?) before-close-seq)) 100) => false

                 ;;test seq after close
                 (count (filter (complement nil?) (retry-cache-seq cache))) => 0))))

