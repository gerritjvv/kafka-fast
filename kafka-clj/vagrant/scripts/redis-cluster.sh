#!/usr/bin/env bash
###
### These:
###   redis


# install redis
echo "Installing redis"

REDIS_VERSION="3.0.0-rc4"
REDIS_NAME="redis-$REDIS_VERSION"

if [ ! -f /var/log/redisinstall.log ];
then

 sudo apt-get -y install daemon
 #redis server requires ruby 1.9.1 or bigger from https://leonard.io/blog/2012/05/installing-ruby-1-9-3-on-ubuntu-12-04-precise-pengolin/
 sudo apt-get -y install ruby1.9.1 ruby1.9.1-dev \
              rubygems1.9.1 irb1.9.1 ri1.9.1 rdoc1.9.1 \
              build-essential libopenssl-ruby1.9.1 libssl-dev zlib1g-dev

 sudo update-alternatives --install /usr/bin/ruby ruby /usr/bin/ruby1.9.1 400 \
                          --slave   /usr/share/man/man1/ruby.1.gz ruby.1.gz \
                          /usr/share/man/man1/ruby1.9.1.1.gz \
                          --slave   /usr/bin/ri ri /usr/bin/ri1.9.1 \
                          --slave   /usr/bin/irb irb /usr/bin/irb1.9.1 \
                          --slave   /usr/bin/rdoc rdoc /usr/bin/rdoc1.9.1

 sudo update-alternatives --config ruby
 sudo update-alternatives --config gem

 sudo gem1.9.1 install redis-server
 sudo gem1.9.1 install redis

  cd /opt
  wget https://github.com/antirez/redis/archive/${REDIS_VERSION}.tar.gz
  mv ${REDIS_VERSION}.tar.gz ${REDIS_NAME}.tar.gz
  tar -xzf ${REDIS_NAME}.tar.gz
  cd /opt/$REDIS_NAME
  make

  id -u somename &>/dev/null || useradd redis
  ln -s /opt/$REDIS_NAME/src/redis-cli /usr/bin/


 if [ ! -f /etc/init.d/redis-cluster ]; then
  cp /vagrant/vagrant/config/redis-cluster-init /etc/init.d/redis-cluster
  chmod +x /etc/init.d/redis-cluster
  update-rc.d redis-cluster defaults
 fi

 cat /vagrant/vagrant/config/redis-cluster.conf > /etc/redis-cluster.conf

 cp /vagrant/vagrant/config/redis-nodes.txt /etc/redis-nodes
 
 mkdir -p /var/log/redis-dat
 chmod -R 777 /var/log/redis-dat
 chown -R redis /var/log/redis-dat

 sysctl -w net.core.somaxconn=65535

 touch /var/log/redisinstall.log
fi



