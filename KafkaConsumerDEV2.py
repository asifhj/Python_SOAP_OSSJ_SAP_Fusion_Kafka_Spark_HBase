#### For all old messages and new messages
__author__ = 'asifj'

import logging
from pykafka import KafkaClient

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

client = KafkaClient(hosts="172.22.147.232:9092,172.22.147.242:9092,172.22.147.243:9092")
print client.topics
topic = client.topics['SAPNotesEvent']
print topic.partitions
consumer = topic.get_simple_consumer()
'''for message in consumer:
    if message is not None:
        print "Offset: "+str(message.offset)+"\tMessage: "+str(message.value)
'''