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

