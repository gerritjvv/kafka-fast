

kafka-clj
==========

fast kafka library implemented in clojure


#Usage

Please note that this library is still under development, any contributions are welcome

```[kafka-clj "0.4.7-SNAPSHOT"]```

## Producer

The ```kafka-clien.client``` namespace contains a ```create-connector``` function that returns a async multi threaded thread safe connector.
One producer will be created per topic partition combination, each with its own buffer and timeout, such that compression can be maximised.


```clojure

(use 'kafka-clj.client :reload)

(def msg1kb (.getBytes (clojure.string/join "," (range 278))))
(def msg4kb (.getBytes (clojure.string/join "," (range 10000))))

(def c (create-connector [{:host "localhost" :port 9092}] {}))
;to send snappy
;(def c (create-connector [{:host "localhost" :port 9092}] {:codec 2}))
;to send gzip
;(def c (create-connector [{:host "localhost" :port 9092}] {:codec 1}))

(time (doseq [i (range 100000)] (send-msg c "data" msg1kb)))

```

## Single Producer 
```clojure
(use 'kafka-clj.produce :reload)

(def d [{:topic "data" :partition 0 :bts (.getBytes "HI1")} {:topic "data" :partition 0 :bts (.getBytes "ho4")}] )
;; each message must have the keys :topic :partition :bts, there is a message record type that can be created using the (message topic partition bts) function
(def d [(message "data" 0 (.getBytes "HI1")) (message "data" 0 (.getBytes "ho4"))])
;; this creates the same as above but using the Message record

(def p (producer "localhost" 9092))
;; creates a producer, the function takes the arguments host and port

(send-messages p {} d)
;; sends the messages asyncrhonously to kafka parameters are p , a config map and a sequence of messages

(read-response p 100)
;; ({:topic "data", :partitions ({:partition 0, :error-code 0, :offset 2131})})
;; read-response takes p and a timeout in milliseconds on timeout nil is returned
```

# Benchmark Producer

Environment:

Network: 10 gigbit
Brokers: 4
CPU: 24 (12 core hyper threaded)
RAM: 72 gig (each kafka broker has 8 gig assigned)
DISKS: 12 Sata 7200 RPM (each broker has 12 network threads and 40 io threads assigned)
Topics: 8

Client: (using the lein uberjar command and then running the client as java -XX:MaxDirectMemorySize=2048M -XX:+UseCompressedOops -XX:+UseG1GC -Xmx4g -Xms4g  -jar kafka-clj-0.1.4-SNAPSHOT-standalone.jar)


Results:
1 kb messag (generated using (def msg1kb (.getBytes (clojure.string/join "," (range 278)))) )

```clojure
(time (doseq [i (range 1000000)] (send-msg c "data" msg1kb)))
;;"Elapsed time: 5209.614983 msecs"
```

191975 K messages per second.

# Metadata and offsets

This is more for tooling and UI(s).

```clojure


(require '[kafka-clj.metadata :refer [get-metadata]])
(def metadata (get-metadata [{:host "localhost" :port 9092}] {}))
(clojure.pprint/pprint metadata)
;; meta data from the brokers {topic [{host port} ...] ... }

(require '[kafka-clj.consumer :refer [get-broker-offsets]])
(require '[kafka-clj.fetch :refer [create-offset-producer])
(def conn {:offset-producers (ref {})})

(get-broker-offsets conn metadata ["test123"] {})
;; sample data {{:host "gvanvuuren-compile", :port 9092} {"test123" ({:offset 0, :error-code 0, :locked false, :partition 0} {:offset 0, :error-code 0, :locked false, :partition 1})}}


```

# Consumer

The consumer depends on redis to hold the partition locks, group management data and the partition offsets.

Redis was chosen over zookeeper because:

*  Redis is much more performant than zookeeper. 
*  Zookeeper was not made to store offsets. 
*  Redis can do group management and distributed locks so using zookeeper does not make sense. 
*  Also zookeeper can be a source of problems when a large amount of offsets are stored or the number of consumers become large, so in the end Redis wins the battle, simple + fast.


The library used for redis is https://github.com/gerritjvv/group-redis
 
```clojure

(require '[kafka-clj.consumer :refer [consumer read-msg]])

;create a consumer using localhost as the bootstrap brokers, you can provide more
;read from the "ping" topic, again you can specify more topics here.
;use-earliest true means that the consumer will start from the latest messages
;locking group management and offsets are all saved in redis.the redis conf and options can be found at
;https://github.com/gerritjvv/group-redis
(def c (consumer [{:host "localhost" :port 9092}] ["ping"] {:use-earliest true :max-bytes 1073741824 :metadata-timeout 60000 :redis-conf {:redis-host "localhost" :heart-beat 10} }))


;create a lazy sequence of messages
(defn lazy-ch [c]
  (lazy-seq (cons (read-msg c) (lazy-ch c))))

;the messages returned are FetchMessage [topic partition ^bytes bts offset locked]

;we flatten here because messages are sent in batches
(doseq [msg (take 10 (flatten (lazy-ch c)))]
  (prn (String. (:bts msg))))
 
```

##Consumer metrics

The api uses http://metrics.codahale.com/ for metrics.
The following metrics are provides

```
kafka-consumer.consume-#[number]        Message consumption per second
kafka-consumer.redis-reads-#[number]    Redis reads per second
kafka-consumer.redis-writes-#[number]   Redis writes per second
kafka-consumer.msg-size-#[number]       Histogram of message byte sizes
kafka-consume.cycle-#[number]           Internal metrics to time each cycle between consume and check for new members
```


```clojure

(require '[kafka-clj.metrics :refer [ report-consumer-metrics ]])
(report-consumer-metrics :console :freq 10) ;report to stdout every 10 seconds
(report-consumer-metrics :csv :freq 10 :dir "/tmp/mydir") ; report to the directory :dir every 10 seconds

```

The metrics registry is held in ```kafka-clj.consumer.metrics-registry```, and can be used to customize reporting



## Kafka Problem solving

For each broker a single fetch message is sent for all topics and partitions on that broker to be consumed.
This means the max-bytes property needs to be big enough to atleast hold one message from each topic partition on that broker, it its smaller
the broker will not send the message and we will get timeouts, also if its too large the brokers will take longer the get all of the data
together, for some reason during production its been noted that the timeout is not always honoured by the broker.

The default value is 52428800 bytes which is 50 mb.

The best is to keep the max-bytes small and limit the size of each message set sent by the producers.

If you get timeouts try first for smaller max-bytes, then bigger 100 mb, 150 mb etc, this will need some experimentation and patience.
 

#Configuration

| Name | Default | Description |
| ---- | ------  | ----------- |
|:bootstrap-brokers | nil | An array of bootstrap brokers from which the consumer and producer will read the initial broker cluster state, e.g. ```[{:host "localhost" :port 9092} {:host "host2" :port 9092}]``` |
|:batch-num-messages | 100  | Number of messages to batch before sending. If should be high enough for performance but not too high so that the total message-set size is too big. |
|:queue-buffering-max-ms | 1000 | Number of milliseconds to wait before sending, if the :batch-num-message has no been reached yet but this timeout happens, then the currently held data will be sent.| 
|:max-wait-time | 1000 | The number of milliseconds the server should wait to gather data (up to at least :min-bytes) for a fetch request. |
|:min-bytes  | 1 | The minimum bytes a server should have before returning a fetch request. |
|:max-bytes  | 52428800 (50mb) | The maximum number of bytes a fetch request should return. |
|:client-id  | "1" | Used for identifying client requests. |
|:codec      | 0   | The compression that should be used for sending messages, 0 = None, 1 = Gzip, 2 = Snappy. |
|:acks       | 1   | The number of replicas that should be written and a response message returned for a produce send. | 
|:offset-commit-freq | 5000 | Offsets consumed will be committed every :offset-commit-freq milliseconds. |
|:fetch-timeout | 30000 | Milliseconds to wait for a broker to response to a fetch request. |
|:use-earliest  | true  | Only applies if no offset is held for a particular topic + partition in redis. If true will use the earliest available offset from the broker, otherwise the latest offset is used. |
|:metadata-timeout  | 10000 | Milliseconds to wait for a broker to respond to a metadata request. |
|:send-cache-max-entries | 1000000 | number of entries to keep in cache for server ack checks |
|:send-cache-expire-after-write | 5 | seconds to expire an entry after write |
|:send-cache-expire-after-access | 5 | seconds to expire an entry after read |


# Produce Error handling and persistence

When sending messages the broker(s) may respond with and error or the broker itself may be down.
In case a broker is down but other brokers are up, the messages will be sent to the 'up' brokers.

If no brokers are available of the broker responds with and error message, the message is saved to a off heap cache.
https://github.com/jankotek/mapdb is used for this purpose.

The latter is only true if ack is not 0. 


## Retry cache logic

Each producer is represented by a producer-buffer, each producer-buffer will send any errors from the tcp client or as a Response error from the broker send the error to
a common producer-error-ch channel.

A go block is created that will read from the producer-error-ch channel and does:

* write-to-retry-cache
* update-metadata
* removes the producer from the global producer-ref
* and closes the producer buffer that sent the error


A background thread is created that will get all of the values from the retry cache 
and re-send it using the send-msg entry method, that will again send the message to 
a different producer buffer, the message once sent is deleted from the retry-cache.

The logic above is created in the create-connector function, and attached to the connector.

The close function will stop all the background logic above.


# Consumer Implementation

This section covers more details about the consumer implementation.

## create-fetch-producer

This method creates a connection with a broker over which fetch requests can be sent, and responses read.

```create-fetch-producer broker conf``` where broker is ```{:broker "host" :port 9092}```.

To send a fetch request call ```send-fetch fetch-producer topic-partitions```, topic-partitions have the format ```[ [topic-name [ {:partition 0 :offset 0} {:partition 1 :offset 0} ...]] ... ]```
Note that the partitions must be held on the broker the request is sent for.

##  read-fetch

To read the response  the read-fetch is used ```read-fetch byte-buff state f```

The function f is applied everytime a message or fetch error is read, and apply as ```(apply f state msg)```,
the state is accumelated as with reduce so that each state is the result of apply f to a previous message (or the initial state).

So to return a list of messages read fetch can be called as ```read-fetch byte-buff [] conj```


## Exmaple

```clojure
(require '[kafka-clj.fetch :refer [send-fetch read-fetch create-fetch-producer]]:reload)
(import 'io.netty.buffer.Unpooled)

(def p (create-fetch-producer {:host "localhost" :port 9092} {}))

(send-fetch p [["ping" [{:partition 0 :offset 0}]]])

(def cs [(-> p :client :read-ch) (-> p :client :error-ch)])
(def vs (alts!! cs))

(read-fetch (Unpooled/wrappedBuffer (first vs)) [] conj )
```


# Commong errors during use

## FetchError error code 1

This means that the offset queried is out of range (does not exist on the broker any more).
It either means that you are starting up a consumer using old offsets, or if you see this message more than on startup it means
that the consumer cannot keepup with the producers, and that data is deleted off the brokers before the consumer could consume it.

The solution to this would be to add more consumers and increase the log.retention.bytes and or log.retention.hours on the brokers.

## Message-set-size  aaa  is bigger than the readable bytes  bbbb

Its common for kafka to send partial messages, not so common to send a whole partial message set. This error if seen infrequently ignore, but if you're 
getting allot of these errors it might point to that your fetch size max bytes is too small and the actual message sets are larger than that value,
for some reason kafka will still send it but partially.

To fix experiment with increasing the value of the property ```kafka.max-bytes``` slowly, one megabyte at a time. If the value is too big  you'll start getting timeouts.

Also check if the messages being sent can be reduced in size.

 
## Contact

Email: gerritjvv@gmail.com

Twitter: @gerrit_jvv

## License


Distributed under the Eclipse Public License either version 1.0 
