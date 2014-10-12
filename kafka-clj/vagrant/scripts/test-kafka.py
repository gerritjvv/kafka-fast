#!/usr/bin/env python

from kafka import KafkaClient, SimpleProducer, SimpleConsumer

# To send messages synchronously

kafka = KafkaClient("192.168.4.40:9092")
producer = SimpleProducer(kafka)


producer.send_messages("my-topic", "some message")
producer.send_messages("my-topic", "this method", "is variadic")


i = 0
consumer = SimpleConsumer(kafka, "my-group", "my-topic")
for message in consumer:
    print(message)
    i=i+1
    if i == 3:
      break

if i == 3:
 print("Congratulations you have a running kafka cluster")

kafka.close()