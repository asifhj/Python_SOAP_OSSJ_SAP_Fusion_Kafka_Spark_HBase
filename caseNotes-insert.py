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
collection = db['srNotes']
collection_new = db['srNotes-new']
document_no = 1969218
documents = collection.find(no_cursor_timeout=True)[1969218:]
inserts = 1969218
updates = 0
for document in documents:
    del document['_id']
    for key, value in document.iteritems():
        try:
            document[key] = str(value).strip()
        except Exception:
            pass
    key = {'caseId': document['SRID']}
    doc = collection_new.find_one({'caseId': document['SRID']})
    if not doc:
        inserts += 1
        doc_to_ins = key
        doc_to_ins['note'] = []
        for key, value in document.iteritems():
            try:
                document[key] = str(value).strip()
            except Exception:
                pass
        doc_to_ins['note'].append(document)
        collection_new.insert(doc_to_ins)
    else:
        updates += 1
        doc['note'].append(document)
        collection_new.update(key, doc, upsert=True);
    #print "Inserts: "+str(inserts)
    #print "Updates: "+str(updates)
    print "Total: "+str(inserts+updates)
