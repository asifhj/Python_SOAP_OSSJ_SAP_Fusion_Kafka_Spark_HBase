__author__ = 'asifj'
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import traceback

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

DB_VM_MONGO_IP = "10.219.48.134"
DB_LOCAL_MONGO_IP = "192.168.56.101"
DB_NAME = "SAPEvent"
DB_PORT = 27017
KAFKA_IP = "172.22.147.242"
KAFAK_PORT = "9020"
KAFKA_TOPIC = "SAPEvent"
url = "http://172.22.147.248:8092/api/"


def drop_database():
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    print client.database_names()
    client.drop_database(DB_NAME)
    client.drop_database(DB_NAME + "Debug")
    print client.database_names()
    client.close()


def upsert_document_debug(coll, doc):
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    db = client[DB_NAME + "Debug"]
    collection = db[coll]
    post_id = collection.insert(doc)
    client.close()
    return post_id


def upsert_document(kafka_collection, kafka_doc):
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    db = client[DB_NAME]
    collection = db[kafka_collection]
    kafka_doc = kafka_doc[kafka_collection]
    key = {'caseId': kafka_doc['caseId']}
    print "Key: " + str(key)
    post_id = ''
    if kafka_collection == 'additionalParams':
        post_id = collection.insert(kafka_doc)
    elif kafka_collection == 'srKbLink':
        mongo_doc = collection.find_one(key)
        if type(kafka_doc['link']) is dict:
            print "KbLink is not array!"
            kafka_doc['link'] = [kafka_doc['link']]
        if type(kafka_doc['link']) is list:
            if kafka_doc['link'][0].get('mode', "") == "":
                return
        if mongo_doc is not None:
            print "hihello"
            #print "Number of kblinks in mongo: " + str(len(mongo_doc['link']))
            #print mongo_doc
            #print "Number of kblinks in kafka: " + str(len(kafka_doc['link']))
            #print kafka_doc
            link_count = 0
            found = 0
            new_mongo_doc = mongo_doc
            for mongo_kblink in mongo_doc['link']:
                #print "mongodict "+str(mongo_kblink)
                for kafka_kblink in kafka_doc['link']:
                    #print "kafkadict "+str(kafka_kblink)
                    if str(mongo_kblink['kbId']) == str(kafka_kblink['kbId']) and str(kafka_kblink['mode']).strip() == "U":
                        print "Updating KbLinks..."
                        new_mongo_doc['link'][link_count] = kafka_kblink
                    if str(mongo_kblink['kbId']) == str(kafka_kblink['kbId']) and str(kafka_kblink['mode']).strip() == "D":
                        print "Deleting KbLinks..."
                        del new_mongo_doc['link'][link_count]
                    if str(mongo_kblink['kbId']) == str(kafka_kblink['kbId']) and str(kafka_kblink['mode']).strip() == "I":
                        print "Inserting KbLinks..."
                        new_mongo_doc['link'].append(kafka_kblink)
                link_count += 1
            del new_mongo_doc['_id']
            post_id = collection.update(key, new_mongo_doc, upsert=True)
        else:
            post_id = collection.insert(kafka_doc)
    else:
        post_id = collection.update(key, kafka_doc, upsert=True)
    client.close()
    return post_id


def upsert_customermaster_document(message):
    client = MongoClient(DB_LOCAL_MONGO_IP, DB_PORT)
    db = client[DB_NAME]
    collection = db["customerMaster"]
    document = ""
    if not message.get("relationship", "") == "":
        if type(message['relationship']) is list:
            accountId = message['relationship'][0]['accountId']
            print "Message for account " + str(accountId) + " received!"
            print "Checking account existence in MongoDB..."
            count = collection.find({"relationship.0.accountId": accountId}).count()
            print "No of documents in MongoDB : " + str(count)
            if count:
                document = collection.find_one({"relationship.0.accountId": accountId})
                kafka_relationships = message['relationship']
                kafka_relationships_count = len(message['relationship'])
                mongo_relationships = document['relationship']
                mongo_relationships_count = len(document['relationship'])
                print "MongoDB relationship count: " + str(mongo_relationships_count)
                print "Kafka relationship count: " + str(kafka_relationships_count)
                print "Updating Relationships..."
                for kafka_relationship in kafka_relationships:
                    print "Mode of operation: " + str(kafka_relationship['mode'])
                    if kafka_relationship['mode'] == "I":
                        print "Insertion operation is taking place"
                        pointer = 0
                        tmp_mongo_relationships = mongo_relationships
                        updated = 0
                        for mongo_relationship in mongo_relationships:
                            if kafka_relationship['accountId'] == mongo_relationship['accountId'] and \
                                            kafka_relationship['contactId'] == mongo_relationship['contactId']:
                                tmp_mongo_relationships[pointer] = kafka_relationship
                                updated = 1
                            pointer += 1
                        if updated == 0:
                            tmp_mongo_relationships.append(kafka_relationship)
                        mongo_relationships = tmp_mongo_relationships
                    if kafka_relationship['mode'] == "D":
                        print "Deletion operation is taking place"
                        pointer = 0
                        tmp_mongo_relationships = mongo_relationships
                        updated = 0
                        for mongo_relationship in mongo_relationships:
                            if kafka_relationship['accountId'] == mongo_relationship['accountId'] and \
                                            kafka_relationship['contactId'] == mongo_relationship['contactId']:
                                del tmp_mongo_relationships[pointer]
                                updated = 1
                            pointer += 1
                        mongo_relationships = tmp_mongo_relationships
                    if kafka_relationship['mode'] == "C":
                        print "Update operation is taking place"
                        pointer = 0
                        tmp_mongo_relationships = mongo_relationships
                        updated = 0
                        for mongo_relationship in mongo_relationships:
                            if kafka_relationship['accountId'] == mongo_relationship['accountId'] and \
                                            kafka_relationship['contactId'] == mongo_relationship['contactId']:
                                tmp_mongo_relationships[pointer] = kafka_relationship
                                updated = 1
                            pointer += 1
                        mongo_relationships = tmp_mongo_relationships
                document['relationship'] = mongo_relationships
                _id = document['_id']
                del document['_id']
                print "DocumentID: " + str(collection.update({'_id': _id}, document, upsert=True))
            else:
                print "Account does not exist in MongoDB, hence adding one..."
                collection.insert(message)
        else:
            print "Relationship is not array!"

    if message.get("header", "") is not "" or message.get("address", "") is not "" or message.get("marketingAttributes",
                                                                                                  "") is not "":
        partnerId = message['header']['partnerId']
        print "Message for partner " + str(partnerId) + " received!"
        print "Checking partner existence in MongoDB..."
        count = collection.find({"header.partnerId": partnerId}).count()
        print "No of documents in MongoDB: " + str(count)
        if count:
            document = collection.find_one({"header.partnerId": partnerId})
            if message.get('address', "") is not "":
                print "Updating Address..."
                document['address'] = message['address']
            if message.get('header', "") is not "":
                print "Updating Header..."
                document['header'] = message['header']
            if message.get('marketingAttributes', "") is not "":
                print "Updating MarketingAttributes..."
                document['marketingAttributes'] = message['marketingAttributes']
            _id = document['_id']
            del document['_id']
            client.close()
            print "DocumentID: " + str(collection.update({'_id': _id}, document, upsert=True))
        else:
            print "Account does not exist in MongoDB, hence adding one..."
            client.close()
            return collection.insert(message)
    else:
        client.close()


print "Cleaning old Mongo docs..."
drop_database()
print "Mongo docs cleaned!"
# To consume messages

consumer = KafkaConsumer(KAFKA_TOPIC,
                         bootstrap_servers=['172.22.147.242:9092', '172.22.147.232:9092', '172.22.147.243:9092'],
                         auto_commit_enable=False, auto_offset_reset="smallest")
# group_id='CLIEvent-grp',
# consumer.configure(bootstrap_servers=['172.22.147.242:9092', '172.22.147.232:9092', '172.22.147.243:9092'], auto_commit_enable=False, auto_offset_reset="smallest")
message_no = 1
for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    topic = message.topic
    partition = message.partition
    offset = message.offset
    key = message.key
    print "Topic: " + str(topic) + ", Partition: " + str(partition) + ", Offset: " + str(offset)
    message = str(message.value)
    #print "================================================================================================================="
    if message is not None:
        try:
            document = json.loads(message)
            #print "Event Type: " + str(document.keys())
            #print "Message No: " + str(message_no)
            collection = document.keys()[0]
            #print "Collection Name: " + str(collection)
            #print "Debug Document ID: " + str(upsert_document_debug(collection, document[collection]))
            document = json.loads(message)
            #if collection == "customerMaster":
            #    print "DocumentID: " + str(upsert_customermaster_document(document['customerMaster']))
            #if collection == "srKbLink":
            if collection == "srKbLink" and document['srKbLink']['caseId'] == "2015-1117-T-0021":
                print "#######################################DocumentID: " + str(upsert_document(collection, document))
        except Exception, err:
            print "CustomException"
            print "Kafka Message: " + str(message)
            print(traceback.format_exc())
    #print "================================================================================================================="
    #print "\n"
    message_no += 1



### KBLINKs testing
''''
kblinks = [{'srKbLink': {'link': [{'kbId': '00000000000000001234', 'status': '', 'description': 'abc', 'internalId': \
    '0002141635', 'url': 'www.abc.com', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}], \
    'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '1234', 'status': '', 'description': 'abc', 'internalId': '0002141635', 'url': 'www.abc.com', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': 'X', 'srVisibility': 'PUBLIC'}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '00000000000000005678', 'status': '', 'description': 'DEF', 'internalId': '0002141636', 'url': 'WWW.DEF.COM', 'kbDate': '20151117142414', 'dataSource': 'DEF', 'sourceVisibility': '', 'integrated': '', 'srVisibility': ''}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '00000000000000001234', 'status': '', 'description': 'abc', 'internalId': '0002141635', 'url': 'www.abc.comqw', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '1234', 'status': '', 'description': 'abc', 'internalId': '0002141635', 'url': 'www.abc.comqw', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}, {'kbId': '5678', 'status': '', 'description': 'DEF', 'internalId': '0002141636', 'url': 'WWW.DEF.COM', 'kbDate': '20151117142414', 'dataSource': 'DEF', 'sourceVisibility': '', 'integrated': '', 'srVisibility': ''}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '00000000000000009012', 'status': '', 'description': 'IJK', 'internalId': '0002141637', 'url': 'WWW.IJK.COM', 'kbDate': '20151117144250', 'dataSource': 'IJK', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '00000000000000001234', 'status': '', 'description': 'abc', 'internalId': '0002141635', 'url': 'www.abc.co.in', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '1234', 'status': '', 'description': 'abc', 'internalId': '0002141635', 'url': 'www.abc.co.in', 'kbDate': '20151117142059', 'dataSource': 'ABC', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}, {'kbId': '5678', 'status': '', 'description': 'DEF', 'internalId': '0002141636', 'url': 'WWW.DEF.COM', 'kbDate': '20151117142414', 'dataSource': 'DEF', 'sourceVisibility': '', 'integrated': '', 'srVisibility': ''}, {'kbId': '9012', 'status': '', 'description': 'IJK', 'internalId': '0002141637', 'url': 'WWW.IJK.COM', 'kbDate': '20151117144250', 'dataSource': 'IJK', 'sourceVisibility': '', 'integrated': '', 'srVisibility': 'PUBLIC'}], 'caseId': '2015-1117-T-0021'}}, {'srKbLink': {'link': [{'kbId': '00000000000000005678', 'status': '', 'description': 'DEF', 'internalId': '0002141636', 'url': 'WWW.DEF.CO.IN', 'kbDate': '20151117142414', 'dataSource': 'DEFG', 'sourceVisibility': '', 'integrated': '', 'srVisibility': ''}], 'caseId': '2015-1117-T-0021'}}]

for kbl in kblinks:
    collection = kbl.keys()[0]
    print "Length of Kblinks in kafka : "+str(len(kbl['srKbLink']['link']))
    print "DocumentID: " + str(upsert_document(collection, kbl))

'''
