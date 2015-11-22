__author__ = 'asifj'

import logging
from kafka import KafkaConsumer
import json
import traceback
from bson.json_util import dumps
from kafka import SimpleProducer, KafkaClient
from utils import Utils


logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

inputs = []
consumer = KafkaConsumer("SAPEvent", bootstrap_servers=['172.22.147.242:9092', '172.22.147.232:9092', '172.22.147.243:9092'], auto_commit_enable=False, auto_offset_reset="smallest")
message_no = 1
inputs = consumer.fetch_messages()
'''for message in consumer:
    topic = message.topic
    partition = message.partition
    offset = message.offset
    key = message.key
    message = message.value
    print "================================================================================================================="
    if message is not None:
        try:
            document = json.loads(message)
            collection = document.keys()[0]
            if collection == "customerMaster":
                print "customerMaster"
            elif collection == "srAttachements":
                #print dumps(document, sort_keys=True)
                inputs.append(document)
        except Exception, err:
            print "CustomException"
            print "Kafka Message: "+str(message)
            print(traceback.format_exc())
    print "================================================================================================================="
    print "\n"
    message_no += 1
'''
# To send messages synchronously
kafka = KafkaClient('172.22.147.232:9092,172.22.147.242:9092,172.22.147.243:9092')
producer = SimpleProducer(kafka)

for i in inputs:
    try:
        #producer.send_messages(b'SAPEvent', json.dumps(input))
        document = json.loads(str(i.value))
        type = document.keys()[0]
        if type == "srDetails":
            print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            row = []
            utils = Utils()
            row = utils.validate_sr_details( document['srDetails'], row)
            print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            print "\n\n"
    except Exception:
        print "Kafka: "+str(document)
        print Exception.message
        print(traceback.format_exc())
