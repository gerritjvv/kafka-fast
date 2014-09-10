kafka-clj
==========

fast kafka library implemented in clojure


Function docs
=============

```

create-organiser!
    group-conn
    meta-producers
    redis-conn
    work-processor

calculate-new-work  [org topics]
    query kafka metadata     
    push to redis

consumer-start [state]
    msg-chan ;only if not in state
    redis-conn
    load-pool ;only if not in state
    producers {}

wait-and-do-work-unit! [consumer f-delegate]
    get-work! ;blocking
    do-work by reading from kafka 
    publish to redis

get-work! => wait-on-work-unit!
    brpoplpush queue working-queue 0

consume! [state]
    create a function that writes resp-data to msg-ch
    add n consumers to load pool i.e work-units will be processed by (-> state :conf :consumer-threads), note that a work-unit will only be processed by one thread
    start publish thread
          will read from redis work-queue + add to working queue
          add publish to the load pool
          where a consumer will pickup the work-unit

publish-work 
    push to redis work-queue

```

Code Examples
==============

Work Organiser
==============
```clojure
(use 'kafka-clj.consumer.work-organiser :reload)

(def org (create-organiser!

 {:bootstrap-brokers [{:host "localhost" :port 9092}]
  :redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))


(calculate-new-work org ["ping"])



(use 'kafka-clj.consumer.consumer :reload)
(def consumer (consumer-start {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}}))

(def res (do-work-unit! consumer (fn [state status resp-data] state)))
````

Consumer
========
```clojure
(use 'kafka-clj.consumer.consumer :reload)
(def consumer {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
(def res (wait-and-do-work-unit! consumer (fn [state status resp-data] state)))

(use 'kafka-clj.consumer.consumer :reload)

(require '[clojure.core.async :refer [go alts!! >!! <!! >! <! timeout chan]])
(def msg-ch (chan 1000))

(def consumer {:redis-conf {:host "localhost" :max-active 5 :timeout 1000} :working-queue "working" :complete-queue "complete" :work-queue "work" :conf {}})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 0 :len 10})
(publish-work consumer {:producer {:host "localhost" :port 9092} :topic "ping" :partition 0 :offset 11 :len 10})

(consume! (assoc consumer :msg-ch msg-ch))

(<!! msg-ch)
(<!! msg-ch)
```

#Integration Testing


The namespace in test ```kafka-clj.util``` contain helper functions to launch  
embedded Kafka and Zookeeper instances and launch the native installed Redis server.  

Not that Redis must be installed locally.  


## Test Template

```clojure
(ns kafka-clj.integration-v2-counts
  (:require [kafka-clj.util :refer [startup-resources shutdown-resources create-topics]]
            [kafka-clj.client :refer [create-connector send-msg close]]
            [kafka-clj.consumer.node :refer [create-node! read-msg! shutdown-node!]])
  (:use midje.sweet))

;Test that we can produce N messages and consumer N messages.
;:it tag for integration tests

(def state-ref (atom nil))
(def node-ref (atom nil))
(def client-ref (atom nil))

(defn- uniq-name []
  (str (System/currentTimeMillis)))

(defn- send-test-messages [c topic n]
  (dotimes [i n]
    (send-msg c topic (.getBytes (str "my-test-message-" i)))))

(defn- setup-test-data [topic n]
  (send-test-messages @client-ref topic n))

(defonce test-topic (uniq-name))

(with-state-changes
  [ (before :facts (do (reset! state-ref (startup-resources test-topic))
                       (reset! client-ref (create-connector (get-in @state-ref [:kafka :brokers]) {}))
                       (comment
                         (reset! node-ref (create-node!
                                            {:bootstrap-brokers (get-in @state-ref [:kafka :brokers])
                                             :redis-conf {:host "localhost"
                                                          :port (get-in @state-ref [:redis :port])
                                                          :max-active 5 :timeout 1000 :group-name (uniq-name)}
                                             :conf {}}
                                            [test-topic])))
                       (Thread/sleep 1000)
                       (setup-test-data test-topic 100000)))
    (after :facts (do
                    (close @client-ref)
                    ; (shutdown-node! @node-ref)
                    (shutdown-resources @state-ref)))]

  (fact "Test message counts" :it
        1 => 1))
        
```