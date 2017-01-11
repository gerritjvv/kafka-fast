#!/bin/bash

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

#bootstrap server
echo "STARTING 3 kafka brokers using $KAFKA_HOME"

rm -f $KAFKA_HOME/config/server.properties
cp /vagrant/vagrant/config/server$1.properties $KAFKA_HOME/config/server.properties


cp /vagrant/vagrant/config/kafka_jaas$1.conf $KAFKA_HOME/config/kafka_jaas.conf
cp /vagrant/vagrant/config/krb5.conf /etc/krb5.conf


echo KAFKA_OPTS="${KAFKA_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=$KAFKA_HOME/config/kafka_jaas.conf" \
      $KAFKA_HOME/bin/kafka-server-start.sh \
      /vagrant/vagrant/config/server$1.properties

KAFKA_OPTS="${KAFKA_OPTS} -Djava.util.logging.config.file=/etc/kafkalogging.properties -Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=$KAFKA_HOME/config/kafka_jaas.conf" \
 $KAFKA_HOME/bin/kafka-server-start.sh \
 /vagrant/vagrant/config/server$1.properties &
