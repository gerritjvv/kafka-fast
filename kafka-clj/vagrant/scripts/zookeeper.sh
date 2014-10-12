#!/bin/bash

# create myid file. see http://zookeeper.apache.org/doc/r3.1.1/zookeeperAdmin.html#sc_zkMulitServerSetup
if [ ! -d /tmp/zookeeper ]; then
    echo creating zookeeper data dir...
    mkdir /tmp/zookeeper
    echo $1 > /tmp/zookeeper/myid
fi

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

#bootstrap server
echo "STARTING zookeer using $KAFKA_HOME"

# echo starting zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh /vagrant/vagrant/config/zookeeper.properties > /tmp/zookeeper.log &

