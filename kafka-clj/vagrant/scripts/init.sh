#!/bin/bash
#download rpm if not present

#ensure vagrant/rpm exists
mkdir -p /vagrant/rpm


KAFKA_DOWNLOAD="http://apache.rediris.es/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz"
KAFKA_FILE="kafka_2.9.2-0.8.1.1.tgz"
KAFKA_DIR="/usr/local/kafka_2.9.2-0.8.1.1"

#ensure vagrant/rpm exists
mkdir -p /vagrant/rpm


if [ ! -f /vagrant/rpm/$KAFKA_FILE ]; then
    echo Downloading kafka...
    wget $KAFKA_DOWNLOAD -P /vagrant/rpm/
fi

if [ ! -d "$KAFKA_DIR" ]; then
    ln -s /usr/java/default/bin/jps /usr/bin/jps

    cp -R /vagrant/rpm/$KAFKA_FILE /usr/local/
    cd /usr/local
    tar -xzf $KAFKA_FILE
    echo "export KAFKA_HOME=$KAFKA_DIR" >> /etc/profile.d/kafka.sh
fi

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

echo "JAVA_HOME=$JAVA_HOME"
echo "KAFKA_HOME=$KAFKA_HOME"

#disabling iptables
/etc/init.d/iptables stop
echo done installing jdk and kafka
# chmod scripts
chmod u+x /vagrant/vagrant/scripts/*.sh
