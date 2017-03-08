#!/usr/bin/env bash


function up-node {

 NODE=$1
 vagrant up --provision $NODE

}

function check-zookeeper {

  IP=$(vagrant ssh-config zookeeper | grep 'HostName' | awk '{print $2}' | tr '\n' ' ' | tr -d '[:space:]')

  MODE=$(echo srvr | nc "$IP" "2181" | grep "Mode")

  if [[ $MODE =~ .*standalone.* ]]
  then

    echo "Zookeeper is running at $IP"

  else
     echo "Zookeeper is not running"
     exit -1
  fi


}

function check-brokers {

  zkip=$(vagrant ssh-config zookeeper | grep 'HostName' | awk '{print $2}' | tr '\n' ' ')

  #test that the node is up by creating a topic
  ## NOT quite complete
  vagrant ssh broker1 -c "/usr/local/kafka_2.10-0.10.1.0/bin/kafka-topics.sh --zookeeper $zkip --create --topic mytest1 --replication-factor 1 --if-not-exists --partitions 1"


}

function check-services {

  vagrant ssh services1 -c "redis-cli PING" | head -n1 || exit -1

}

function up-nodes {

 for node in zookeeper broker1 broker2 broker3 services1 client; do

     up-node $node || exit -1

 done

}


up-nodes
check-zookeeper
check-brokers
check-services

echo "OK"
