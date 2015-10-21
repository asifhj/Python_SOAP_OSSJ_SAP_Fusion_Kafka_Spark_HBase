__author__ = 'asifj'
from kafka import SimpleProducer, KafkaClient
import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)
# To send messages synchronously
kafka = KafkaClient('192.168.56.101:9092')
producer = SimpleProducer(kafka)

# Note that the application is responsible for encoding messages to type bytes
producer.send_messages(b'test', b'some message')
producer.send_messages(b'test', b'this method', b'is variadic')

# Send unicode message
producer.send_messages(b'test', u'?????'.encode('utf-8'))