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
