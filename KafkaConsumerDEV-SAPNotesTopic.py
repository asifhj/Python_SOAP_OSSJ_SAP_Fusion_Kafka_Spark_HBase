__author__ = 'asifj'
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
import re
import json
import traceback
import sys

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)


DB_VM_MONGO_IP = "10.219.48.134"
DB_LOCAL_MONGO_IP = "192.168.56.101"
DB_NAME = "SAPNotesTopic"
DB_PORT = 27017
KAFKA_IP = "172.22.147.242"
KAFAK_PORT = "9020"
KAFKA_TOPIC = "SAPNotesTopic"
url = "http://172.22.147.248:8092/api/"


def drop_database():
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    client.drop_database(DB_NAME)
    client.close()


def upsert_document_debug(coll, doc):
    client = MongoClient(DB_LOCAL_MONGO_IP , DB_PORT)
    db = client[DB_NAME]
    collection = db[coll+"Debug"]
    post_id = collection.insert(doc[coll])
    client.close()
    return post_id


def upsert_document(coll, doc):
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    db = client[DB_NAME]
    collection = db[coll]
    key = {'caseId': doc[coll]['caseId']}
    print key
    doc = doc[coll]
    post_id = collection.update(key, doc, upsert=True);
    client.close()
    return post_id

print "Cleaning old Mongo docs..."
drop_database()
print "Mongo docs cleaned!"
# To consume messages
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[KAFKA_IP+':9092'], auto_commit_enable=False, auto_offset_reset="smallest")
# group_id='CLIEvent-grp',
#consumer.configure(bootstrap_servers=['172.22.147.242:9092', '172.22.147.232:9092', '172.22.147.243:9092'], auto_commit_enable=False, auto_offset_reset="smallest")
message_no = 1
for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    #print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                     message.offset, message.key,
    #                                     message.value))
    topic = message.topic
    partition = message.partition
    offset = message.offset
    key = message.key
    message = message.value
    print "================================================================================================================="
    if not message is None:
        try:
            document = json.loads(message)
            print "Event Type: "+str(document.keys())
            print "Message No: "+str(message_no)
            collection = document.keys()[0]
            print "Debug Document ID: "+str(upsert_document_debug(collection, document))
            document = json.loads(message)
            print upsert_document(collection, document)
        except Exception, err:
            print "CustomException"
            print "Kafka Message: "+str(message)
            print(traceback.format_exc())
    print "================================================================================================================="
    print "\n"
    message_no += 1
