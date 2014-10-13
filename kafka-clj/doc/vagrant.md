Kafka Vagrant Development
==========================

Home: https://github.com/gerritjvv/kafka-fast

# Overview

Using vagrant allows you to develop kafka application as if your running a full production deployed cluster.

Note: you need vagrant installed before running it :) (https://www.vagrantup.com/)

## Machines/Boxes

The boxes launched are:

*Brokers*
  * broker1 192.168.4.40:9092
  * broker2 192.168.4.41:9092
  * broker3 192.168.4.42:9092
  
*Zookeeper*
  * zookeeper1 192.168.4.2:2181
  
*Services* -- Redis
  * redis 192.168.4.10 
  
The services box is there to not only run the redis instance but any other instances such as a mysql db etc that  
is required for a particular usecase.

## Startup

To run type:

```vagrant up```

To destroy all boxes run:

```vagrant destroy```

## What next?

Once the boxes are up and running you can refer to them and use them as any other kafka cluster.

But first test that the cluster is up and running by running ping on each of the ips above.


### Send data to the cluster:

Remember to create the topic first using vagrant/scripts/create_topic_remote.sh "my-topic"

*Clojure*

```clojure
(use 'kafka-clj.client :reload)

(def msg1kb (.getBytes (clojure.string/join "," (range 278))))

(def c (create-connector [{:host "192.168.4.40" :port 9092}] {}))

(send-msg c "my-topic" msg1kb)
```

*Java*

```java
import kakfa_clj.core.*;

Producer producer = Producer.connect(new BrokerConf("192.168.4.40", 9092));
producer.sendMsg("my-topic", "Hi".getBytes("UTF-8"));
producer.close();
```

### Consume data from the cluster

Remember to create the topic first using vagrant/scripts/create_topic_remote.sh "my-topic"

*Clojure*

```clojure
(use 'kafka-clj.consumer.node :reload)
(def consumer-conf {:bootstrap-brokers [{:host "192.168.4.40" :port 9092}] :redis-conf {:host "192.168.4.10" :max-active 5 :timeout 1000 :group-name "test"} :conf {}})
(def node (create-node! consumer-conf ["my-topic"]))

(read-msg! node)
;;for a single message
(def m (msg-seq! node))

```

*Java*

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