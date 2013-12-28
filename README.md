

kafka-fast
==========

fast kafka send library implemented in clojure


#Usage

Please note that this library is still under development, any contributions are welcome

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
