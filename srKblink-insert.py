__author__ = 'asifj'

import requests
from pymongo import MongoClient
import json
import csv
import traceback
import logging
from tabulate import tabulate
from bson.json_util import dumps

client = MongoClient('10.219.48.134', 27017)
#client = MongoClient('192.168.56.101', 27017)
db = client['ImportedEvents']
collection = db['srKbLink']
collection_new = db['srKbLink-new']
document_no = 0
documents = collection.find(no_cursor_timeout=True)
inserts = 0
updates = 0
for document in documents:
    for key, value in document.iteritems():
        document[key] = str(value).strip()
    key = {'caseId': document['SRID']}
    doc = collection_new.find_one({'caseId': document['SRID']})
    del document['_id']
    if not doc:
        inserts += 1
        doc_to_ins = key
        doc_to_ins['link'] = []
        for key, value in document.iteritems():
            document[key] = str(value).strip()
        doc_to_ins['link'].append(document)
        collection_new.insert(doc_to_ins)
    else:
        updates += 1
        doc['link'].append(document)
        collection_new.update(key, doc, upsert=True);
    print "Inserts: "+str(inserts)
    print "Updates: "+str(updates)
