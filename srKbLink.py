__author__ = 'asifj'
import requests
from pymongo import MongoClient
import json
import csv
import traceback
import logging
from tabulate import tabulate

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

class HBase:
    def __init__(self):
        self.url = "http://172.22.147.248:8092/api/"
        pass

    def get_case_by_case_id(self, document, row):
        print "API URL: "+self.url+"case-manager/cases/"+str(document['caseId'])
        r = requests.get(self.url+"case-manager/cases/"+str(document['caseId']))
        print "CaseID: "+str(document['caseId'])
        print "Response: "+str(r.status_code)
        #keys = len(document.keys())
        #print "Keys: "+str(keys)
        row.append(r.status_code)
        status = 0
        if r.status_code==200:
            response = json.loads(r.text)
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                print "Incorrect value for 'caseId'!"
                status = 1
            document_kbLinks_len = len(document['link'])
            response_kbLinks_len = 0
            if type(document['link']) is dict:
                print "kBLinks in document is not an array!"
                document_kbLinks_len = 1
                document['link'] =  [document['link']]
            if response['kbLinks'] is not None:
                response_kbLinks_len = len(response['kbLinks'])
            else:
                response_kbLinks_len = 0

            print "Number of kbLinks in document: "+str(document_kbLinks_len)
            print "Number of kbLinks in API response: "+str(response_kbLinks_len)

            if document_kbLinks_len==0:
                print "No kbLinks found in document!"
                row.append("No kbLinks found in document!")
                print "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                print "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                return row
            if response_kbLinks_len==0 and document_kbLinks_len>0:
                print "No kbLinks found in API response but present in document."
                row.append("No kbLinks found in API response but present in document.")
                print "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                print "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                return row

            for doc_link in document['link']:
                match_level = 0
                found = 0
                match_location = 0
                counter = 0
                old_match_level = 0
                match_data = ""
                for resp in response['kbLinks']:
                    match_level = 0
                    if doc_link['kbId'] == ("" if resp['kbId'] is None else resp['kbId']):
                        match_level += 1
                    if doc_link['status'] == ("" if resp['status'] is None else resp['status']):
                        match_level += 1
                    if doc_link['description'] == ("" if resp['description'] is None else resp['description']):
                        match_level += 1
                    if doc_link['internalId'] == ("" if resp['internalId'] is None else resp['internalId']):
                        match_level += 1
                    if doc_link['url'] == ("" if resp['url'] is None else resp['url']):
                        match_level += 1
                    if doc_link['kbDate'] == ("" if resp['kbDate'] is None else str(resp['kbDate']).replace("-", "").replace(":", "").replace(" ", "")):
                        match_level += 1
                    if doc_link['dataSource'] == ("" if resp['dataSource'] is None else resp['dataSource']):
                        match_level += 1
                    if doc_link['sourceVisibility'] == ("" if resp['srcVisiblity'] is None else resp['srcVisiblity']):
                        match_level += 1
                    if doc_link['integrated'] == ("" if resp['kbFlag'] is None else resp['kbFlag']):
                        match_level += 1
                    if doc_link['srVisibility'] == ("" if resp['srVisibility'] is None else resp['srVisibility']):
                        if match_level >= 9:
                            found = 1
                            match_level += 1
                            match_location = counter
                            match_data = resp
                            break;
                    if match_level >= old_match_level:
                        match_location = counter
                        old_match_level = match_level
                        match_data = resp
                    counter += 1
                if found == 0:
                    print "************************************************"
                    print "Data Mismatch, max number of values matched is "+str(old_match_level)
                    print "Kafka ==> "+str(json.dumps(doc_link, sort_keys=True))
                    print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", ""]
                    tmp.append("Incorrect value for 'kbLinks'!")
                    table.append(tmp)
                    status = 1
                    print "************************************************"
                else:
                    print "Data matched, highest level of match is "+str(match_level)
                    print "Kafka ==> "+str(json.dumps(doc_link, sort_keys=True))
                    print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", ""]
                    tmp.append("Match found for 'kbLinks'!")
                    table.append(tmp)

            if status == 0:
                print "Match Found"
                row.append("Match Found")
            else:
                print "\nCompared JSONs"
                print "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                print "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                print tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            print "No Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        return row

client = MongoClient('10.219.48.134', 27017)
#client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['srKbLink']
api = HBase()
document_no = 0
#documents = collection.find({})
documents = collection.find({ 'caseId': '2015-1117-T-0021'})
ofile = open('srKbLink.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
row = ["SNo", "CaseID", "KafkaJSON", "APIResponse", "Status"]
writer.writerow(row)
for document in documents:
    row = []
    document_no += 1
    row.append(document_no)
    row.append(document['caseId'])
    row.append(str(document).replace("\n", ""))
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "Document No: "+str(document_no)
    try:
        row = api.get_case_by_case_id(document, row)
    except Exception:
        print Exception.message
        print(traceback.format_exc())
    writer.writerow(row)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "\n\n"
ofile.close()

