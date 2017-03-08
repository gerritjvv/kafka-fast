#!/usr/bin/env bash

#### Run the producing code
#### Requires that the client, zookeeper, broker{1..3}, services1 boxes are running
#### see startup.sh

#### Expects the kafka-clj project to be synced to client@/vagrant/

if [ -z "$3" ]; then
 echo "cmd <topic> <threads> <count-per-thread>"
 exit -1
fi

BRK1=$(vagrant ssh-config broker1 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')
BRK2=$(vagrant ssh-config broker2 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')
BRK3=$(vagrant ssh-config broker3 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')


TOPIC=$1
THREADS=$2
COUNT_PER_THREAD=$3


echo "Getting zk ip"
zkip=$(vagrant ssh-config zookeeper | grep 'HostName' | awk '{print $2}' | tr '\n' ' ')

echo "Got zk ip $zkip"

echo "Creating topic $TOPIC"
vagrant ssh broker1 -c "/usr/local/kafka_2.10-0.10.1.0/bin/kafka-topics.sh --zookeeper $zkip --create --topic $TOPIC --replication-factor 2 --if-not-exists --partitions 2"


echo "Running lein run send on client $BRK1,$BRK2,$BRK3 $TOPIC $THREADS $COUNT_PER_THREAD"
vagrant ssh client -c "cd /vagrant; LEIN_ROOT=true lein with-profile vagrant-digital run send $BRK1,$BRK2,$BRK3 $TOPIC $THREADS $COUNT_PER_THREAD"

echo "done"