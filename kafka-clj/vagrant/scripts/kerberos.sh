#!/usr/bin/env bash
###
### Install MIT kerberos

echo "Installing kerberos"

sudo apt-get install -y byacc bison
sudo apt-get install -y wget

KRB5DIR="/usr/local/var/krb5kdc"

## increase entropy on the current system, otherwise kerberos utils hangs while generating keys
sudo apt-get install -y rng-tools
sudo rngd -r /dev/urandom

if [ ! -d "$KRB5DIR" ]; then

    #https://bugs.launchpad.net/ubuntu/+source/krb5/+bug/1332988
    sudo apt-get purge krb5-kdc krb5-admin-server

    wget http://web.mit.edu/kerberos/dist/krb5/1.15/krb5-1.15.tar.gz
    tar -zxf krb5-1.15.tar.gz

    cd krb5-1.15/src

    ./configure && make

    sudo make install

    sudo mkdir -p "$KRB5DIR"

    sudo cp /vagrant/vagrant/config/krb5.conf  /etc/krb5.conf
    sudo cp /vagrant/vagrant/config/kdc.conf  "$KRB5DIR/kdc.conf"
    sudo cp /vagrant/vagrant/config/kadm5.acl "$KRB5DIR/kadm5.acl"
    sudo cp /vagrant/vagrant/config/kadm5.dict "$KRB5DIR/kadm5.dict"

    sudo chown root "$KRB5DIR"
    sudo chmod 700 "$KRB5DIR"

    #create soft link, debian wants krb to be under /var/lib/krb5kdc
    #see http://comp.protocols.kerberos.narkive.com/gVop58J2/missing-parms-in-kdc-conf
    sudo ln -s "$KRB5DIR" /var/lib/

    #make sure the principle db is removed
    sudo rm -f "$KRB5DIR"/.k5.KAFKAFAST
    sudo rm -f "$KRB5DIR"/principal
    sudo rm -f "$KRB5DIR"/principal.kadm5
    sudo rm -f "$KRB5DIR"/principal.kadm5.lock
    sudo rm -f "$KRB5DIR"/principal.ok

    #create master key pwd and save to stash file, the kdc database is encrypted using this pwd
    sudo /usr/local/sbin/kdb5_util create -s -P abc

    #administrative principle
    sudo /usr/local/sbin/kadmin.local addprinc -pw abc root/admin
    sudo /usr/local/sbin/kadmin.local addprinc -pw abc vagrant/admin

    sudo /usr/local/sbin/kadmin.local ktadd -k "$KRB5DIR"/kadm5.keytab kadmin/admin kadmin/changepw

    for i in {1..3}
    do
        sudo /usr/local/sbin/kadmin.local -q "addprinc -randkey kafka/broker${i}.kafkafast@KAFKAFAST"
        sudo /usr/local/sbin/kadmin.local -q "ktadd -k /vagrant/vagrant/keytabs/broker${i}.keytab kafka/broker${i}.kafkafast@KAFKAFAST"
    done

    sudo /usr/local/sbin/kadmin.local listprincs
else
    echo "kerberos is already installed"
fi


sudo krb5kdc
sudo kadmind
