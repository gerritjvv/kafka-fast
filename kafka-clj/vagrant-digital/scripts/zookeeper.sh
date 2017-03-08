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
IP=$(/sbin/ifconfig eth0 | grep "inet" | head -n1 | awk '{print $2}')

ZK_CONF_FILE=/vagrant/config/zookeeper.properties

sed "s;{ip};${IP};g" $ZK_CONF_FILE > /tmp/zookeeper.properties

$KAFKA_HOME/bin/zookeeper-server-start.sh /tmp/zookeeper.properties > /tmp/zookeeper.log &

