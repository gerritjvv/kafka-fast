# Vagrant on digital ocean

## Overview

The aim is to allow running and testing kafka-fast off a local machine onto multiple VMs,
and run performance testing. These scripts can also allow someone to get quickly up and running
in a cloud environment for testing.

## Machines

```
CPU	RAM	slug

8	16	16gb	Client
2	4	4gb	    Broker
2	2	2gb	    ZK
2	2	2gb   	Redis
```

The actual IP addresses are assigned when created.

## Configure

Add the ```~/.vagrant.d/config.yaml``` file with contents:

```
configs:
  use: 'default'
  default:
    provider_token: <api-token>
    region: <region>
```

## Log output

/usr/local/kafka_2.10-0.10.1.0/logs/server.log

## Scripts

All operation are performed in the ```kafka-clj/vagrant-digital``` folder, and
are executed locally (no need to ssh into the machines)

* start.sh *

Starts all the droplets in order.

* produce.sh *

Send test json messages to kafka, from the client vm.

```produce.sh <topic> <threads> <count-per-thread>```

* consume.sh *

Read messages as fast as it can from a kafka topic.

```consume.sh <topic>```


## Testing


### Test 1

Run the producer.sh and consumer.sh at the same time.


Simulate network issues from a client to a certain broker.

Install dsniff and run ```tcpkill host <broker-ip>```,
this will kill all tcp connections goin gto the broker.

Result:

The connector should blacklist this broker and only retry it every 10 seconds.


