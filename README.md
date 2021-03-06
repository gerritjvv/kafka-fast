kafka-clj
==========

fast kafka library for any JVM language implemented in clojure 

The documentation contains examples in both clojure and java.  
From the Java APIs you can use Scala, JRuby, Groovy etc.  

Note that at the moment only the *public* producer and consumer APIs have direct Java interfaces,  
internal APIs like direct producer access and direct metadata access are for the moment only in clojure,
albeit you can still access them from Java using the clojure.lang.RT object.  

This project contains a Vagrant template that allows you to tryout a full kafka cluster deploy on your local machine,  
See https://github.com/gerritjvv/kafka-fast/blob/master/kafka-clj/doc/vagrant.md

[![Build Status](https://travis-ci.org/gerritjvv/kafka-fast.svg)](https://travis-ci.org/gerritjvv/kafka-fast)

# Kafka versions tested

*  0.8.2.x
*  0.9.0.x
*  0.10.0.0
*  0.10.1.0

# Usage

## Leiningen

[![Clojars Project](http://clojars.org/kafka-clj/latest-version.svg)](http://clojars.org/kafka-clj)

## Maven

```xml
<dependency>
  <groupId>kafka-clj</groupId>
  <artifactId>kafka-clj</artifactId>
  <version>${kafka-clj.version}</version>
</dependency>
<repositories>
  <repository>
    <id>clojars</id>
    <url>http://clojars.org/repo/</url>
  </repository>
</repositories>
```

## Kerberos		
 		
 For Kerberos support see: https://github.com/gerritjvv/kafka-fast#jaas-sasl		
 
## Creating Topics

In production and for testing its best practice to create the topics manually (or via an automated script) rather than try and use the Kafka Producer to "auto-create" it. Care should be taken and some thought given to the number of partitions and the replication factor. Although this library in particular is not bounded by the number of partitions it is still a means by which the Kafka Brokers split data internally and affects data deletion and replication.

## Producer

The ```kafka-client.client``` namespace contains a ```create-connector``` function that returns a async multi threaded thread safe connector.
One producer will be created per topic partition combination, each with its own buffer and timeout, such that compression can be maximised.


### Clojure

```clojure

(use 'kafka-clj.client :reload)

(def msg1kb (.getBytes (clojure.string/join "," (range 278))))

;;use flush-on-write true for testing, this will flush the message on write to kafka
;;set to false for performance in production
(def c (create-connector [{:host "localhost" :port 9092}] {:flush-on-write true}))

;to send snappy
;(def c (create-connector [{:host "localhost" :port 9092}] {:codec 2}))
;to send gzip
;(def c (create-connector [{:host "localhost" :port 9092}] {:codec 1}))

(time (doseq [i (range 100000)] (send-msg c "data" msg1kb)))

```

### Java

```java
import kakfa_clj.core.*;

Producer producer = Producer.connect(new BrokerConf("192.168.4.40", 9092));
producer.sendMsg("my-topic", "Hi".getBytes("UTF-8"));
producer.close();

```


## Single Producer

*Note:* 

Only use this if you need fine gain control over to which producer a message is sent,  
for normal random distribution use the kafka-clj.client namespace. 

### Clojure

```clojure
(use 'kafka-clj.produce :reload)

(def d [{:topic "data" :partition 0 :bts (.getBytes "HI1")} {:topic "data" :partition 0 :bts (.getBytes "ho4")}] )
;; each message must have the keys :topic :partition :bts, there is a message record type that can be created using the (message topic partition bts) function
(def d [(message "data" 0 (.getBytes "HI1")) (message "data" 0 (.getBytes "ho4"))])
;; this creates the same as above but using the Message record

(def p (producer "192.168.4.40" 9092 {:acks 1}))
;; creates a producer, the function takes the arguments host and port

(dotimes [i 10000] (send-messages p {} d))
;; sends the messages asynchronously to kafka parameters are p , a config map and a sequence of messages

(read-response p 100)
;; note that this function tries its best to not block but may still block during the actual IO read
;; (#kafka_clj.response.ProduceResponse{:correlation-id 22, :topic "data", :partition 0, :error-code 3, :offset -1})
;; read-response takes p and a timeout in milliseconds on timeout nil is returned
```

Note:
 The send-messages function has two arity versions:
 ```(send-messages producer conf msgs)``` and ```(send-messages connector producer conf msgs)```  

 The connector must contain ```{:send-cache (kafka-clj.msg-persist/create-send-cache)}```, by default if not provided  
 the global ```kafka-clj.produce/global-message-ack-cache``` instance is used.  
 

# Benchmark Producer

Environment:  

Network: 10 gigabit  
Brokers: 4  
CPU: 24 (12 core hyper threaded)  
RAM: 72 gig (each kafka broker has 8 gig assigned)  
DISKS: 12 Sata 7200 RPM (each broker has 12 network threads and 40 io threads assigned)  
Topics: 8  
  
Client: (using the lein uberjar command and then running the client as java -XX:MaxDirectMemorySize=2048M -XX:+UseCompressedOops -XX:+UseG1GC -Xmx4g -Xms4g  -jar kafka-clj-0.1.4-SNAPSHOT-standalone.jar)  
  
  
Results:  
1 kb message (generated using (def msg1kb (.getBytes (clojure.string/join "," (range 278)))) )  
  

```clojure
(time (doseq [i (range 1000000)] (send-msg c "data" msg1kb)))
;;"Elapsed time: 5209.614983 msecs"
```
  
191975 K messages per second.  
  
# Metadata and offsets

This is more for tooling and UI(s).

```clojure


(require '[kafka-clj.metadata :refer [get-metadata]])
(require '[kafka-clj.produce :refer [metadata-request-producer]])
(require '[kafka-clj.consumer.util :refer [get-broker-offsets]])

(def metadata-producer (metadata-request-producer "localhost" 9092 {}))

(def meta (get-metadata [metadata-producer] {}))

;;{"test123" [{:host "gvanvuuren-compile", :port 9092} {:host "gvanvuuren-compile", :port 9092}]


(def offsets (get-broker-offsets {:offset-producers (ref {})} meta ["test"] {:use-earliest false}))

;;{{:host "gvanvuuren-compile", :port 9092} {"test" ({:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 0} {:offset 7, :all-offsets (7 0), :error-code 0, :locked false, :partition 1})}}


```

# Consumer

The consumer depends on redis to hold the partition locks, group management data and the partition offsets.

Redis was chosen over zookeeper because:

*  Redis is much more performant than zookeeper. 
*  Zookeeper was not made to store offsets. 
*  Redis can do group management and distributed locks so using zookeeper does not make sense. 
*  Also zookeeper can be a source of problems when a large amount of offsets are stored or the number of consumers become large, so in the end Redis wins the battle, simple + fast.

For HA Redis (Cluster Redis) see: 

https://github.com/gerritjvv/kafka-fast/blob/master/kafka-clj/doc/redis-cluster.md


## Connection Pooling

All consumer tcp connections are pooled.
The default is set to 20 which might be quite high for some application, to change set the ```:pool-limit <num>``` 
in the consumer config.

## Load balancing

A work queue concept is used to share the load over several consumers. 
A master is automatically selected between the consumers, the master will run the work-organiser which is responsible for calculating and publishing work to redis.
Each consumer will read and consume messages from the redis work queue.

## Offsets and consuming earliest

Note that if no data is saved in redis the consumer will take the latest offset from kafka and set it to the topic in redis, then start consumption from that position.  
This can be changed by setting the :use-earliest property to true. It is normally recommended to leave this property at false, run the consumer and then start producing messages.  

## Consuming topics

### Clojure

```clojure

(use 'kafka-clj.consumer.node :reload)
(def consumer-conf {:bootstrap-brokers [{:host "localhost" :port 9092}] :redis-conf {:host "localhost" :max-active 5 :timeout 1000 :group-name "test"} :conf {}})
(def node (create-node! consumer-conf ["ping"]))

(read-msg! node)
;;for a single message
(def m (msg-seq! node))
;;for a lazy sequence of messages

(add-topics! node ["test1" "test2"])
;;add topics
(remove-topics! node ["test1"])
;;remove topics

;;when the consumer node is closed m will return nil after the last message,
;;this allows for reading till closed blocking if waiting for messages and not shutdown
;(doseq [msg (take-while (complement nil?) m)]
; (prn (:topic msg) " " (:partition msg) " " (:offset msg) " " (:bts msg)))

(shutdown-node! node)
;;closes the consumer node

```

### Java

The consumer instance returned by Consumer.connect and all of its methods are thread safe.

```java
import kakfa_clj.core.*;

Consumer consumer = Consumer.connect(new KafkaConf(), new BrokerConf[]{new BrokerConf("192.168.4.40", 9092)}, new RedisConf("192.168.4.10", 6379, "test-group"), "my-topic");
Message msg = consumer.readMsg();

String topic = msg.getTopic();
long partition = msg.getPartition();
long offset = msg.getOffset();
byte[] bts = msg.getBytes();

//Add topics
consumer.addTopics("topic1", "topic2");

//Remove topics
consumer.removeTopics("topic1", "topic2");

//Iterator: Consumer is Iterable and consumer.iterator() returns a threadsafe iterator
//          that will return true unless the consumer is closed.
for(Message message : consumer){
  System.out.println(message);
}

//close
consumer.close();

```

## Vagrant

Vagrant allows you to run a whole kafka cluster with zookeeper and redis all on your local machine.  
For testing this is one of the best things you can do and makes testing kafka + new features easy.  

See: https://github.com/gerritjvv/kafka-fast/blob/master/kafka-clj/doc/vagrant.md

## Monitoring

A kafka consumer can have a considerable memory footprint due to buffering and background fetching going on (all done for the sake of performance).  
To see where and how memory is used several functions are provided in the consumer namespace.   

For byte sizes the openjkd jol (http://openjdk.java.net/projects/code-tools/jol/) project is used. 
This works with any JVM including the Oracle JVM.  

### MSG Channel

The consumer message channel is an intermediate buffer and can contain N amount of messages that take up place in memory before being consumed.
Note that the size will vary as messages move through the channel.  

A measure if how many bytes can be obtained using:

```clojure

(require '[kafka-clj.consumer.node :as node])

(node/msg-chan-byte-size consumer-node)
;; the total deep size in bytes occupied by the message channel

```

### Consumer TCP Pools and Redis/Kafka Fetch Threads

Fetching data from Kafka uses  

  * Thread pool for fetching work units from redis ( default 1 )
  * Thread pool for fetching data from kafka ( default 2 )
  * TCP Pool for connections to kafka

```clojure
(require '[kafka-clj.consumer.node :as node])

(consumer/node-stats consumer-node)
;; {:exec-service <consume-thread-pool-stats>
;;  :conn-pool <tcp-pool-stats>
;;  :fetch-stats <map key=thread-name val={:ts <time-wu-was-seen> :duration <fetch-time-ms> :wu <work-unit>}>
;;  :node-stats {:offsets-ahead-stats {<topic> {<partition> {:saved-offset <offset-in-redis> :max-offset <last-max-offset-from-kafka> :date <date>}} }}}
```

### Redis Connection Pools

When using a non clustered redis install, a Redis connection pool is used for performance and better failover.
To monitor how many active and idle connections are alive in the system use:

```clojure
(require '[kafka-clj.consumer.node :as node])

(node/conn-pool-idle consumer-node)
;; number of idle connections

(node/conn-pool-active consumer-node)
;; number of active connections

(node/conn-pool-byte-size consumer-node)
;; the total deep size in bytes occupied by the whole redis pool

```

### Consumer Work Units and monitoring

Each consumer will process work units as they become available on the work queue. 
When a work unit has been completed by the consumer an event is sent to the work-unit-event-ch channel (core.async channel).

Note that the work-unit-event-ch channel is a sliding channel with a buffer or 100, meaning events not consumed will be lost.

These events can be saved to disk and analysed later to gain more insight into what is being processed by each host and how fast,
it can also help to debug a consumer.

To get the channel use:

```clojure
(def event (<!! (:work-unit-event-ch node)))
```

The event format is:
```clojure
{:event "done"
 :ts ts-millis
 :wu {:seen ts-millis
      :topic topic
      :partition partition
      :producer {:host host :port port}
      :offset offset
      :len len
      :offset-read offset-read
      :status status
     }
}
```

See https://github.com/gerritjvv/kafka-fast/tree/master/kafka-events-disk for writing events to disk

# JAAS SASL

This connector supports SASL_PLAINTEXT. To activate specify ```:conf :jaas``` e.g ```:conf :jaas "KafkaClient"```.

Note that the SASL mechanism in kafka 0.9.0 is not compatible with that of 0.10.0. When using 0.9.0 specify 
```:conf :kafka-version "0.9.0" ```

#Configuration

## Clojure

| Name | Default | Description |
| ---- | ------  | ----------- |
|:bootstrap-brokers | nil | An array of bootstrap brokers from which the consumer and producer will read the initial broker cluster state, e.g. ```[{:host "localhost" :port 9092} {:host "host2" :port 9092}]``` |
|:batch-num-messages | 100000  | Number of messages to batch before sending. If should be high enough for performance but not too high so that the total message-set size is too big. |
|:queue-buffering-max-ms | 1000 | Number of milliseconds to wait before sending, if the :batch-num-message has no been reached yet but this timeout happens, then the currently held data will be sent.| 
|:conf :batch-byte-limit | 10485760 (10 MB) | If the number of bytes in a batch is bigger than this limit the batch will be sent to kafka |
|:conf :batch-fail-message-over-limit | true | If a single message is over this limit it will not be sent, and error message printed and the message is discarded |
|:conf :max-wait-time | 1000 | The number of milliseconds the server should wait to gather data (up to at least :min-bytes) for a fetch request. |
|:conf :min-bytes  | 1 | The minimum bytes a server should have before returning a fetch request. |
|:conf :max-bytes  | 5000000 (~5mb) | The maximum number of bytes a fetch request should return, as of version 3.5.5 this value is auto tuned i.e it starts at the defined value in the configuration and then adds if not all messages could be consumed in a single fetch and removes if too many bytes were read for non work unit messages. |
|:conf :client-id  | "1" | Used for identifying client requests. |
|:conf :codec      | 0   | The compression that should be used for sending messages, 0 = None, 1 = Gzip, 2 = Snappy. |
|:conf :acks       | 1   | The number of replicas that should be written and a response message returned for a produce send. | 
|:conf :offset-commit-freq | 5000 | Offsets consumed will be committed every :offset-commit-freq milliseconds. |
|:conf :fetch-timeout | 30000 | Milliseconds to wait for a broker to response to a fetch request. |
|:conf :use-earliest  | false  | Only applies if no offset is held for a particular topic + partition in redis. If true will use the earliest available offset from the broker, otherwise the latest offset is used. |
|:conf :metadata-timeout  | 10000 | Milliseconds to wait for a broker to respond to a metadata request. |
|:conf :send-cache-max-entries | 1000000 | number of entries to keep in cache for server ack checks |
|:conf :send-cache-expire-after-write | 5 | seconds to expire an entry after write |
|:conf :send-cache-expire-after-access | 5 | seconds to expire an entry after read |
|:conf :consume-step | 100000 | The max number of messages to consume in a single work unit |
|:conf:redis-conf | ```:redis-conf {:host "localhost" :max-active 10 :timeout 500}``` | The redis configuration for the consumer, to use sentinel do {:master-name "sentinel-master" :sentinel-addresses ["host1" "host2"]} |
|:conf :reset-ahead-offsets | ```:reset-ahead-offsets true``` default is false | If the brokers during restarts report a lower offset than is saved, we reset the read offset to that of the max reported broker offset for a topic/partition see https://github.com/gerritjvv/kafka-fast/issues/10 |
|:conf :consumer-threads | 2 | number of background threads doing fetching from kafka |
|:conf :redis-fetch-threads | 1 | number of background threads that fetch workunits from redis parallel |
|:conf :consumer-reporting | false | if true the kafka consumer will print out metrics for the number of messages sent the the msg-ch every 10 seconds |
|:conf :consumer-conn-max-total | 40 | The maximum number of connections that should be created |
|:conf :consumer-conn-max-total-per-key | 40 | The maximum number of connections that should be created per broker |
|:conf :msg-ch-buff-size | 100 | The buffer size between the background fetching threads and the calling consumer api |
|:conf :work-unit-event-ch-buff-size | The buffer (sliding window) of work unit events |
|:conf :jaas | The name to use inside the jaas file configuration |
|:conf :kafka-version | The kafka version used e.g 0.9.0 will indicate that the SASL 0.9.0 compatible handshake should be used |

### Performance configuration for consuming

An easy way to see how fast you are consuming via Kafka is by enabling ```:consumer-reporting```, this will print out message consumption metrics
every 10 seconds.  

**Background Fetch Threads**

Sometimes if might be as simple as increasing the number of ```:consumer-threads```, try 4, 6 threads.  
Normally if you see that you can keepup with processing kafka messages from the connector even if the queue contains allot of messages still  
this is a sign that you are not fetching fast enough.  

**MSG Channel Buffer**

The message buffer between fetching threads and the calling code is 100 by default. Depending on the message size etc and memory usage this value 
can be reduced. Increasing it doesnt seem to increase performance.

## Java 

For configuration options with the Java API see the ```kakfa_clj.core.KafkaConf``` class

# Bootstrap errors

If when a node or connector is created no metadata can be retreived from the kafka brokers, and exception  
is thrown, this generally signals an error with the bootstrap metadata brokers.  

Note: on clean clusters where there are no topics created yet, this might throw a false exception for consumers.


# Produce Error handling and persistence

When sending messages the broker(s) may respond with and error or the broker itself may be down.
In case a broker is down but other brokers are up, the messages will be sent to the 'up' brokers.

If no brokers are available of the broker responds with and error message, the message is saved to a off heap cache.
https://github.com/jankotek/mapdb is used for this purpose.

The latter is only true if ack is not 0. 

## Producer Errors 

### Errors in the ```send-msg``` function

The send-msg function has retry logic to search for better metadata and a broker that will accept the message, if no broker  
is found or no metadata can be read this function throws a ```RuntimeException```.  

It is the responsibility of the calling code to handle this error.  
The best recommendation is to pause and retry (waiting for kafka to sort its metadata)  or write the message to disk  
and or fail the application.  


### Background errors

How the connector handlers producer errors is determined by the ```producer-retry-strategy``` key passed to the ```kafka-clj.client/create-connector``` function  

By default its ```(= producer-retry-strategy :default)```  

if ```:default```  
 The producer will write failed messages (only those that failed while sending to the broker in the background) to a retry cache on disk  
 the messages will be continuously retried in search of brokers that will accept them.

else
  
 The user has to handle any background producer errors by reading the ```producer-error-ch```  
 Producer errors can be read via the "producer-error-ch" channel, the function ```producer-error-ch``` can be used to  
 get the channel from a connector.  
 Note that this channel if not cleared fast enough will block the connector's producers.

The error message sent to the channel is:

```clojure
{:key-val (str topic ":" partition) :error e 
 :producer {:producer producer :buff-ch buff-ch}
 :offset offset :v v :topic topic}
```

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



## create-fetch-producer

This method creates a connection with a broker over which fetch requests can be sent, and responses read.

```create-fetch-producer broker conf``` where broker is ```{:broker "host" :port 9092}```.

To send a fetch request call ```send-fetch fetch-producer topic-partitions```, topic-partitions have the format ```[ [topic-name [ {:partition 0 :offset 0} {:partition 1 :offset 0} ...]] ... ]```
Note that the partitions must be held on the broker the request is sent for.

##  read-fetch

To read the response  the read-fetch is used ```read-fetch byte-buff state f```

The function f is applied every time a message or fetch error is read, and apply as ```(apply f state msg)```,
the state is accumulated as with reduce so that each state is the result of apply f to a previous message (or the initial state).

So to return a list of messages read fetch can be called as ```read-fetch byte-buff [] conj```


## Example

```clojure
(require '[kafka-clj.fetch :refer [send-fetch read-fetch create-fetch-producer]]:reload)
(import 'io.netty.buffer.Unpooled)

(def p (create-fetch-producer {:host "localhost" :port 9092} {}))

(send-fetch p [["ping" [{:partition 0 :offset 0}]]])

(def cs [(-> p :client :read-ch) (-> p :client :error-ch)])
(def vs (alts!! cs))

(read-fetch (Unpooled/wrappedBuffer (first vs)) [] conj )
```


# Common errors during use

## FetchError error code 1

This means that the offset queried is out of range (does not exist on the broker any more).
It either means that you are starting up a consumer using old offsets, or if you see this message more than on startup it means
that the consumer cannot keep up with the producers, and that data is deleted off the brokers before the consumer could consume it.

The solution to this would be to add more consumers and increase the log.retention.bytes and or log.retention.hours on the brokers.

## Message-set-size  aaa  is bigger than the readable bytes  bbbb

Its common for kafka to send partial messages, not so common to send a whole partial message set. This error if seen infrequently ignore, but if you're 
getting allot of these errors it might point to that your fetch size max bytes is too small and the actual message sets are larger than that value,
for some reason kafka will still send it but partially.


Also check if the messages being sent can be reduced in size.

 
## Contact

Email: gerritjvv@gmail.com

Twitter: @gerrit_jvv

## License


Distributed under the Eclipse Public License either version 1.0 
