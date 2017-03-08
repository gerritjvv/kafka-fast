#!/usr/bin/env bash

#### Run the producing code
#### Requires that the client, zookeeper, broker{1..3}, services1 boxes are running
#### see startup.sh

#### Expects the kafka-clj project to be synced to client@/vagrant/
#### see vagrant rsync-auto client

if [ -z "$1" ]; then
 echo "cmd <topic>"
 exit -1
fi

echo "Reading broker1 ip"

BRK1=$(vagrant ssh-config broker1 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')

echo "Reading broker2 ip"

BRK2=$(vagrant ssh-config broker2 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')

echo "Reading broker3 ip"

BRK3=$(vagrant ssh-config broker3 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')


TOPIC="$1"


echo "Getting zk ip"
zkip=$(vagrant ssh-config zookeeper | grep 'HostName' | awk '{print $2}' | tr '\n' ' ')

echo "Got zk ip $zkip"

echo "Creating topic $topic"
vagrant ssh broker1 -c "/usr/local/kafka_2.10-0.10.1.0/bin/kafka-topics.sh --zookeeper $zkip --create --topic $TOPIC --replication-factor 2 --if-not-exists --partitions 2"

echo "Getting redis ip"
REDIS=$(vagrant ssh-config services1 | grep 'HostName' | awk '{print $2}' | tr '\n' ' ')

echo "Running lein run consume on client $BRK1,$BRK2,$BRK3 $REDIS $TOPIC"
vagrant ssh client -c "cd /vagrant; LEIN_ROOT=true lein with-profile vagrant-digital run consume $BRK1,$BRK2,$BRK3 $REDIS $TOPIC"

echo "done"