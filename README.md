

kafka-fast
==========

fast kafka send library implemented in clojure


#Usage

Please note that this library is still under development, any contributions are welcome

## Multi Producer

The ```kafka-clien.client``` namespace contains a ```create-connector``` function that returns a async multi threaded thread safe connector.
One producer will be created per topic partition combination, each with its own buffer and timeout, such that compression can be maximised.


```clojure

(use 'kafka-clj.client :reload)

(def msg1kb (.getBytes (clojure.string/join "," (range 278))))
(def msg4kb (.getBytes (clojure.string/join "," (range 10000))))

(def c (create-connector [{:host "localhost" :port 9092}] {}))

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


