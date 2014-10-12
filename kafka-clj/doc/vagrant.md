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
g

### Send data to the cluster:

```clojure
(use 'kafka-clj.client :reload)

(def msg1kb (.getBytes (clojure.string/join "," (range 278))))

(def c (create-connector [{:host "192.168.4.40" :port 9092}] {}))

(send-msg c "data" msg1kb)
```

### Consume data from the cluster

```clojure
(use 'kafka-clj.consumer.node :reload)
(require '[clojure.core.async :as async])
(def consumer-conf {:bootstrap-brokers [{:host "192.168.4.40" :port 9092}] :redis-conf {:host "192.168.4.2" :max-active 5 :timeout 1000 :group-name "test"} :conf {}})
(def node (create-node! consumer-conf ["ping"]))

```