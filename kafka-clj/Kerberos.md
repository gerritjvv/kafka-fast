# Kerberos authentication


# Overview


## Kafka kerberos implementation

https://github.com/apache/kafka/pull/334/files#diff-b6b91bd51fc3d3d54fb330e658bd890d

## Background reading


Read first what Kerberos is, the first 3 chapters would give you a good overview to understand
the material online:

https://www.amazon.es/Kerberos-Definitive-Guide-Guides/dp/0596004036/ref=sr_1_2?ie=UTF8&qid=1481106704&sr=8-2&keywords=kerberos

Then read how to do Kerberos with Java

http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/


# Kafka Protocol

The kafka protocol is described in: https://kafka.apache.org/protocol

The request is summarised as:

```

Request:

SaslHandshake API (Key: 17):

1. SizeInBytes => int16
2. api_key => INT16      (0)    17
3. api_version => INT16  (0)
4. correlation_id => INT32
5. client_id => NULLABLE_STRING
6. mechanism => String  "GSSAPI" or "PLAIN"


Response:

1. SizeInBytes => int16
2. correlation_id => INT32
3. error_code => INT16      0 => None, 34 => InvalidSaslState, 35 => UnsupportedVersion
4. enabled_mechanisms => [STRING]



```

Steps:

1. Kafka SaslHandshakeRequest containing the SASL mechanism for authentication is sent by the client.

2. If the requested mechanism is not enabled in the server, the server responds with the list of supported mechanisms and closes the client connection. If the mechanism is enabled in the server, the server sends a successful response and continues with SASL authentication.

3. The actual SASL authentication is now performed. A series of SASL client and server tokens corresponding to the mechanism are sent as opaque packets. These packets contain a 32-bit size followed by the token as defined by the protocol for the SASL mechanism.

4. sequent packets are handled as Kafka API requests. Otherwise, the client connection is closed.


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

# Vagrant

vagrant plugin install vagrant-hostmanager


# Client testing

The easiest is to use the credentials that exist on one of the broker hosts already.

Log into broker1 ```vagrant ssh broker1``` and ```cd /vagrant``` ```lein repl```


# Issues and Odities

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