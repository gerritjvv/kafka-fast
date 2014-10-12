#!/usr/bin/env bash

## Runs the create_topic script on the remote zookeeper1 box

vagrant ssh zookeeper1 -c "/vagrant/vagrant/scripts/create_topic.sh $1"