#!/usr/bin/env bash
###
### These:
###   redis


# install redis
echo "Installing redis"

if [ ! -f /var/log/redisinstall.log ];
then


 cd /root/
 wget http://download.redis.io/releases/redis-2.8.14.tar.gz
 tar -xzf redis-2.8.14.tar.gz
 mv /root/redis-2.8.14 /opt/
 cd /opt/redis-2.8.14
 make

 id -u somename &>/dev/null || useradd redis
 ln -s /opt/redis-2.8.14/src/redis-cli /usr/bin/

 if [ ! -f /etc/init.d/redis ]; then
  cp /vagrant/vagrant/config/redis-init /etc/init.d/redis
 fi

 mkdir -p /var/log/redis-dat
 chmod -R 777 /var/log/redis-dat

 touch /var/log/redisinstall.log
fi

if [ -f /etc/redis.conf ];
then
 mv /etc/redis.conf /etc/redis.conf-old
fi

chmod +x /etc/init.d/redis

cp /vagrant/vagrant/config/redis.conf /etc/
chkconfig redis on
service redis restart

