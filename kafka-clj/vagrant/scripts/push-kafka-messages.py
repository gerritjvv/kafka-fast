#!/usr/bin/env python

####Takes three arguments n, topic, and message
####Sends the message n times to kafka
####All messages are sent in sync so that when the script completes all messages have been sent

import sys
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

# To send messages synchronously

n=int(sys.argv[1])
topic=sys.argv[2]
msg=sys.argv[3]

kafka = KafkaClient("192.168.4.40:9092")
producer = SimpleProducer(kafka)


print("sending " + str(n) + " messages to " + str(topic))

for i in range(0, n):
  producer.send_messages(topic, msg)

print("Sent messages")

print("Startup pseidon etl")

kafka.close()