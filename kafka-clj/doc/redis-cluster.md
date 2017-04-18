# Redis Cluster

Home: https://github.com/gerritjvv/kafka-fast


# Overview

Since: `3.0.0-SNAPSHOT`

This document describe how to install Redis cluster for HA fail over for kafka-clj' offset management.

For a more detailed overview of Redis cluster please see:

http://redis.io/topics/cluster-tutorial
http://redis.io/topics/cluster-spec


# Installation


Note: you need gcc installed.

Run `yum install gcc` before running the installation scripts.

Redis cluster instances are started in the Vagrant services1 instance.

An instalation file is provided in :

`$PROJECT_HOME/vagrant/scripts/redis-cluster.sh`

and configuration in:

`$PROJECT_HOME/vagrant/config/redis-cluster.conf`


# Configuration

The minimum configuration required is:

```
port 7000
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
```


# Client library configuration and usage

```
:redis-conf {:host ["server1:6379" "server2:6379" "server3:6379"
                    "server4:6379" "server5:6379" "server6:6379"] :group-name "mygroup"}
```

Example:

```clojure
(use 'kafka-clj.consumer.node :reload)
(def consumer-conf {:bootstrap-brokers [{:host "192.168.4.40" :port 9092}]
                    :redis-conf {:host ["192.168.4.10:6379" "192.168.4.10:6380" "192.168.4.10:6381"
                                        "192.168.4.10:6382" "192.168.4.10:6383" ] :max-active 5 :timeout 1000 :group-name "test"} :conf {}})

(def node (create-node! consumer-conf ["my-topic"]))

```

# Startup

The `redis-cluster-init` script reads ports from the `/etc/redis-nodes` file and starts a redis instance for each port.



# The Code

The redis namespaces have been divided into:

`kafka-clj.redis.core`
`kafka-clj.redis.protocol`
`kafka-clj.redis.cluster`
`kafka-clj.redis.single`

The protocol `IRedis` has been created to abstract what implementation is used inside the kafka-clj code.

Two implementations exist: 1 for a single redis server, and another for a redis cluster.
The correct implementation is chosen in `kafka-clj.redis.core/create` based on if a map with `{:host "host" :port 4444}`
is provided a single Redis is expected, and if {:host ["host:port" "host:port"]} a Redis cluster is expected.

`kafka-clj.consumer.work-organiser/create-organiser!` takes a key argument `redis-factory` which is a function that
creates the redis connection and defaults to `kafka-clj.redis.core/create`

To inject your own implementation of `IRedis` pass in a function that returns this implementation to the consumer which
will pass it to the above function.

The function should have the format:

```clojure
(defn my-function [redis-conf] )
```





