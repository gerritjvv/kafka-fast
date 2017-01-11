#!/bin/bash
########################################################################
################# Install Java and Kafka locally  ######################
#################  called from Vagrantfile        ######################
########################################################################


### Box precise64

## from http://serverfault.com/questions/500764/dpkg-reconfigure-unable-to-re-open-stdin-no-file-or-directory
export DEBIAN_FRONTEND=noninteractive

## Install java
### From https://rais.wordpress.com/2015/03/16/setting-up-a-vagrant-java-8-environment/

sudo apt-get update -y
sudo apt-get install -y software-properties-common python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java

## update packages
sudo apt-get update -y


## required to auto accept the license
## from http://stackoverflow.com/questions/19275856/auto-yes-to-the-license-agreement-on-sudo-apt-get-y-install-oracle-java7-instal
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections

## install java 8
sudo apt-get install -y oracle-java8-installer
sudo apt-get install -y oracle-java8-set-default

## setup java home
JAVA_HOME=/usr/lib/jvm/java-8-oracle/

if ! grep -q -F 'JAVA_HOME' /etc/profile.d/java.sh; then
 echo "Setting up java home to $JAVA_HOME"
 sudo bash -c "echo export JAVA_HOME=$JAVA_HOME >> /etc/profile.d/java.sh"

fi

### Install leiningen

if [ ! -f /bin/lein ]; then
  wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
  sudo cp lein /bin/lein
  sudo chmod +x /bin/lein
  /bin/lein -v
fi

### Install Kafka

#KAFKA_DOWNLOAD="http://apache.uvigo.es/kafka/0.9.0.0/kafka_2.10-0.9.0.0.tgz"
#KAFKA_FILE="kafka_2.10-0.9.0.0.tgz"
#KAFKA_DIR="/usr/local/kafka_2.10-0.9.0.0"

KAFKA_DOWNLOAD="http://apache.rediris.es/kafka/0.10.1.0/kafka_2.10-0.10.1.0.tgz"
KAFKA_FILE="kafka_2.10-0.10.1.0.tgz"
KAFKA_DIR="/usr/local/kafka_2.10-0.10.1.0"


#ensure vagrant/rpm exists
mkdir -p /vagrant/rpm


if [ ! -f /vagrant/rpm/$KAFKA_FILE ]; then
    echo Downloading kafka...
    wget --no-check-certificate $KAFKA_DOWNLOAD -O "/vagrant/rpm/$KAFKA_FILE"
fi

if [ ! -d "$KAFKA_DIR" ]; then
    ln -s $JAVA_HOME/bin/jps /usr/bin/jps

    cp -R /vagrant/rpm/$KAFKA_FILE /usr/local/
    cd /usr/local
    tar -xzf $KAFKA_FILE
    echo "export KAFKA_HOME=$KAFKA_DIR" >> /etc/profile.d/kafka.sh
fi

. /etc/profile.d/java.sh
. /etc/profile.d/kafka.sh

echo "JAVA_HOME=$JAVA_HOME"
echo "KAFKA_HOME=$KAFKA_HOME"

echo done installing jdk and kafka
# chmod scripts
chmod u+x /vagrant/vagrant/scripts/*.sh
