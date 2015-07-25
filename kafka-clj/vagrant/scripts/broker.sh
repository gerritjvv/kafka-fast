#!/bin/bash

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

#bootstrap server
echo "STARTING 3 kafka brokers using $KAFKA_HOME"

rm -f $KAFKA_HOME/config/server.properties
cp /vagrant/vagrant/config/server$1.properties $KAFKA_HOME/config/server.properties

$KAFKA_HOME/bin/kafka-server-start.sh /vagrant/vagrant/config/server$1.properties &