#!/usr/bin/env bash
###
### These:
###   redis


# install redis
echo "Installing redis"

if [ ! -f /var/log/redisinstall.log ];
then

 REDIS_VERSION="3.0.0-rc4"
 REDIS_NAME="redis-$REDIS_VERSION"
 cd /root/
 if [ ! -f ${REDIS_VERSION} ];
 then
  wget https://github.com/antirez/redis/archive/${REDIS_VERSION}.tar.gz
  mv ${REDIS_VERSION} ${REDIS_NAME}.tar.gz
  tar -xzf ${REDIS_NAME}.tar.gz
  mv /root/$REDIS_NAME /opt/
  cd /opt/$REDIS_NAME
  make

  id -u somename &>/dev/null || useradd redis
  ln -s /opt/$REDIS_NAME/src/redis-cli /usr/bin/
 fi

 if [ ! -f /etc/init.d/redis-cluster ]; then
  cp /vagrant/vagrant/config/redis-cluster-init /etc/init.d/redis-cluster
  chmod +x /etc/init.d/redis-custer
 fi

 cat /vagrant/vagrant/config/redis-cluster.conf > /etc/redis-cluster.conf

 cp /vagrant/vagrant/config/redis-nodes.txt /etc/redis-nodes
 
 mkdir -p /var/log/redis-dat
 chmod -R 777 /var/log/redis-dat

 wget -O ruby-install-0.5.0.tar.gz https://github.com/postmodern/ruby-install/archive/v0.5.0.tar.gz
 tar -xzvf ruby-install-0.5.0.tar.gz
 cd ruby-install-0.5.0/
 sudo make install

 #install ruby gems with older version of ruby
 yum -y install rubygems

 #install latest version of ruby
 ruby-install ruby



 gem install redis


 touch /var/log/redisinstall.log
fi



