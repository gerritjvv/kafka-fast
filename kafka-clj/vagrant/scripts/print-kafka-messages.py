#!/usr/bin/env python

####Takes two arguments topic n
#### n is the expected number of messages after which the app will stop reading
####Read the messages from the topic and print each message on a new line

import uuid
import sys
from kafka import KafkaClient, MultiProcessConsumer


topic=sys.argv[1]
n=int(sys.argv[2])

kafka = KafkaClient("192.168.4.40:9092")


consumer = MultiProcessConsumer(kafka, str(uuid.uuid4()), topic)

for msg in consumer.get_messages(count=n, block=True, timeout=60000):
 print(msg)


kafka.close()