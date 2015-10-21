__author__ = 'asifj'

import pymongo
from pymongo import MongoClient
import datetime

class DBManager:
    def __init__(self):
        data = '{"kbId": "PRSEARCH2", "internalId": "0000002552", "dataSource": "PRSEARCH", "status": "", "description": "test description for kblink2", "kbDate": 20151005105655, "srVisibility": "PUBLIC", "sourceVisibility": "", "url": "www.prsearch2.net/test2/"}'
        pass

    def insert_record(self, data):
        client = MongoClient('10.204.95.208', 27017)
        db = client['test']
        tests = db.tests
        post_id = tests.insert_one(data).inserted_id
        return post_id





