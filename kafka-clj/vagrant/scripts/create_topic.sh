#!/bin/bash

#RUN FROM zookeeper1

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

if [ $# -gt 0 ]; then
    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost --replication-factor 1 --partition 2 --topic $1
else
    echo "Usage: create_topic.sh <topic_name>"
fi

