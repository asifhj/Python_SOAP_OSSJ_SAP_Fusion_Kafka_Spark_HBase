__author__ = 'asifj'
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
import re
import json
import traceback
import sys
from bson import Binary, Code
from bson.json_util import dumps

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

IP = "192.168.56.101"
DB = "SAPEvent"
PORT = 27017
url = "http://172.22.147.248:8092/api/"

document = {
    "relationship": [
        {
            "relationshipCreateTime": 131959,
            "relationshipChangeDate": "",
            "validTo": 99991231,
            "relationshipCreateDate": 20151014,
            "relationshipCode": "BUR011",
            "contactId": "0000008154",
            "mode": "I",
            "relationshipDescription": "Has the Employee Responsible",
            "validFrom": "00010101",
            "accountId": 100197496,
            "relationshipChangeTime": ""
        },
        {
            "relationshipCreateTime": 124853,
            "relationshipChangeDate": 20151014,
            "validTo": 99991231,
            "relationshipCreateDate": 20151014,
            "relationshipCode": "BUR011",
            "contactId": "0000037825",
            "mode": "I",
            "relationshipDescription": "Has the Employee Responsible",
            "validFrom": "00010101",
            "accountId": 100197496,
            "relationshipChangeTime": 131959
        },
        {
            "relationshipCreateTime": 124853,
            "relationshipChangeDate": 20151014,
            "validTo": 99991231,
            "relationshipCreateDate": 20151014,
            "relationshipCode": "BUR011",
            "contactId": "0000037825",
            "mode": "D",
            "relationshipDescription": "Has the Employee Responsible",
            "validFrom": "00010101",
            "accountId": 100197496,
            "relationshipChangeTime": 131959
        }
    ]
}

def drop_database():
    client = MongoClient(IP, PORT)
    client.drop_database(DB)

def upsert_document(message):
    client = MongoClient(IP, PORT)
    db = client[DB]
    collection = db["customerMaster"]
    rel = message.get("relationship", "")
    head = message.get("header", "")
    add = message.get("address", "")
    relationship = {}
    header = {}
    address = {}
    for key in message:
        print "key: %s" % (key)

    if rel is not "":
        if type(message['relationship']) is list:
            i = 0
            docs = 0
            document = {}
            for rel in message['relationship']:
                mode = rel['mode']
                accountId = int(rel['accountId'])
                contactId = rel['contactId']
                docs = collection.find({"relationship."+str(i)+".accountId" : accountId, "relationship."+str(i)+".contactId" : contactId}).count()
                print {"relationship."+str(i)+".accountId" : accountId, "relationship."+str(i)+".contactId" : contactId}
                i += 1
                if docs==1:
                    print "found"
                    cursor = collection.find({"relationship."+str(i)+".accountId" : accountId, "relationship."+str(i)+".contactId" : contactId})
                    #cursor = collection.find({})
                    for doc in cursor:
                        document = doc
                    break
                elif docs>1:
                    print "Found multiple docs"
                print "\n"

            print dumps(document, indent=4)
            for relation in document['relationship']:
                mode = relation['mode']
                accountId = int(relation['accountId'])
                contactId = relation['contactId']
                i = 0
                if mode=="I":
                    document['relationship'].append(relation)
                if mode=="D":
                    for item in document['relationship']:
                        if item['contactId'] == contactId and item['accountId'] == accountId and mode == "D":
                            del document['relationship'][i]
                            break
                        i += 1
                '''i = 0
                if mode=="s":
                    for item in document['relationship']:
                        if item['contactId'] == contactId and item['accountId']==accountId and mode == "s":
                            document['relationship'][i] = relation
                        i += 1'''
                print mode
            print dumps(document, indent=4)



        else:
            print "relationship is not array!"

print upsert_document(document)
