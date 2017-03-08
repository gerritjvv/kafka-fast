#!/bin/bash

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

ID="$1"
ZK="$2"

if [ -z "$ZK" ]; then

 echo "provide script <id> <zk-address>"
 exit

fi

#bootstrap server
echo "STARTING $ID kafka broker using $KAFKA_HOME"

PROPS_FILE="/vagrant/config/server.properties"

rm -f $KAFKA_HOME/config/server.properties
cp /vagrant/config/server.properties $KAFKA_HOME/config/server.properties

IP=$(/sbin/ifconfig eth0 | grep "inet" | head -n1 | awk '{print $2}')

sed "s;{ip};$IP;g" $PROPS_FILE | sed "s;{id};${ID};g" | sed "s;{zk};${ZK};g" > /tmp/server.properties

KAFKA_OPTS="${KAFKA_OPTS} -Djava.util.logging.config.file=/etc/kafkalogging.properties" \
 $KAFKA_HOME/bin/kafka-server-start.sh \
 /tmp/server.properties &
