# Kerberos authentication



## Background reading


Read first what Kerberos is, the first 3 chapters would give you a good overview to understand
the material online:

https://www.amazon.es/Kerberos-Definitive-Guide-Guides/dp/0596004036/ref=sr_1_2?ie=UTF8&qid=1481106704&sr=8-2&keywords=kerberos

Then read how to do Kerberos with Java

http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/


# Kafka Protocol

The kafka protocol is described in: https://kafka.apache.org/protocol


# Configuration

The jaas conf should be specified as a java property and a krb5.conf file

See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html

```
-Djava.security.auth.login.config=/etc/kafka/jaas.conf
-Djava.security.krb5.conf=/etc/kafka/krb5.conf
```


## Jaas file format

http://kafka.apache.org/documentation.html#security_kerberos_sasl_clientconfig

```
KafkaClient {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/etc/security/keytabs/kafka_client.keytab"
        principal="kafka-client-1@EXAMPLE.COM";
    };
```


### Consumer and Producer properties

```
:security-protocol=SASL_PLAINTEXT (or SASL_SSL)
:sasl-mechanism=GSSAPI
:sasl-kerberos-service-name=kafka
```

### Kafka kerberos implementation

https://github.com/apache/kafka/pull/334/files#diff-b6b91bd51fc3d3d54fb330e658bd890d


# Vagrant


```vagrant plugin install vagrant-hostmanager```


# Client testing

The easiest is to use the credentials that exist on one of the broker hosts already.

Log into broker1 ```vagrant ssh broker1``` and ```cd /vagrant``` ```lein repl```


# Issues and Odities

First of all, enable debug output for kerberos using:

```
-Dsun.security.krb5.debug=true
-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext
```

## Kerberos is enabled and the client or node hangs or timesout

Enable debug on one of the kafka brokers contacted, the ```GssKrb5Server``` is used by the kafka broker to handle kerbeors authentication,
and uses the java logging api. To enable trace log level do:

 *  edit ```./vagrant/scripts/broker.sh``` and add  ```-Djava.util.logging.config.file=/etc/kafkalogging.properties```
 *  create the file ```/etc/kafkalogging.properties``` and insert
    ```
    handlers= java.util.logging.ConsoleHandler, java.util.logging.FileHandler
    .level= FINEST

    java.util.logging.FileHandler.pattern = %h/kafka%u.log
    java.util.logging.FileHandler.limit = 50000
    java.util.logging.FileHandler.count = 1
    java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter

    java.util.logging.ConsoleHandler.level = ALL
    ```

 * on the client side set debug level logging for ```kafka_clj.jaas```

Look for hex byte message output, these should match from the client to the broker.
The broker should perform handshake1 then handshake2 and finally print ```FINE: KRB5SRV12:Authzid: ... ```

## Subject and javax.security.auth.useSubjectCredsOnly

If the Sasl/createSaslClient is not run within the Subject's doAs method
that is retreived from the LoginContext, the credentials will not be picked up.

You can get around this by setting javax.security.auth.useSubjectCredsOnly=false and then
configuring the jaas config section com.sun.security.jgss.krb5.initiate:

e.g
```
com.sun.security.jgss.krb5.initiate {
   com.sun.security.auth.module.Krb5LoginModule required
   doNotPrompt=true
   useTicketCache=true
   useKeyTab=true
   keyTab="/vagrant/vagrant/keytabs/broker1.keytab"
   principal="kafka/broker1.kafkafast@KAFKAFAST";
 };
```

The best way is to always run all sasl client access inside the Subjects's doAs method.

see: http://stackoverflow.com/questions/33829017/gssexception-no-valid-credentials-provided-mechanism-level-failed-to-find-any/41132319#41132319