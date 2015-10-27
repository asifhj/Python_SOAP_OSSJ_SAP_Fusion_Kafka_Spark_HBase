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
        rel = document.get("relationship", "")
        head = document.get("header", "")
        add = document.get("address", "")
        relationship = {}
        header = {}
        address = {}
        for key in document:
            print "key: %s" % (key)

        if rel is not "":
            print len(document['relationship'])
            if type(document['relationship']) is list:
                for rel in document['relationship']:
                    print self.url+"user-manager/users/"+str(rel['accountId'])
                    data = {'accountId' : str(rel['accountId'])}
                    headers = {'content-type': 'application/json'}
                    r = requests.post(self.url+"user-manager/users/", data=json.dumps(data), headers=headers)
                    print "Response: "+str(r.status_code)
                    print r.text
                    response = json.loads(r.text)
                    if len(response["userList"]) > 0:
                        exit()

        '''print self.url+"user-manager/accounts/"+str(document['caseId'])
        r = requests.get(self.url+"user-manager/accounts/"+str(document['caseId']))
        print "CaseID: "+str(document['caseId'])
        print "Response: "+str(r.status_code)
        keys = len(document.keys())
        print "Keys: "+str(keys)
        row.append(r.status_code)
        status = 0
        if r.status_code==200:
            response = json.loads(r.text)
            #print json.dumps(document['link'], indent=4)
            #print json.dumps(response['kbLinks'], indent=4)
            #print document
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                print "Incorrect value for 'caseId'!"
                status = 1

            response_kbLinks_len = len(response['kbLinks'])
            document_kbLinks_len = len(document['link'])
            print cmp(response['kbLinks'], document['link'])
            i = 0
            res = {}
            res['kbLinks'] = {}
            for item in response['kbLinks']:
                res['kbLinks'][str(i)] = item
                i+=1
            print json.dumps(res, indent=4)

            i = 0
            doc = {}
            doc['link'] = {}
            for item in document['link']:
                doc['link'][str(i)] = item
                i+=1
            print json.dumps(doc, indent=4)
            print set(res['kbLinks']["0"].items()) & set(doc['link']["0"].items())

            return
            print document_kbLinks_len
            print response_kbLinks_len
            #print json.dumps(document['link'], indent=4)
            #print json.dumps(response['kbLinks'], indent=4)
            for i in range(0, document_kbLinks_len):
                
                #print json.dumps(document['link'][i], indent=4)
                #print json.dumps(response['kbLinks'][i], indent=4)
                if not document['link'][i]['kbId'] == response['kbLinks'][document_kbLinks_len-1-i]['kbId']:
                    tmp = [i]
                    tmp.append("kbId")
                    tmp.append(document['link'][i]['kbId'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['kbId'])
                    tmp.append("Failed")
                    table.append(tmp)
                    

                if not document['link'][i]['status'] == "" if response['kbLinks'][document_kbLinks_len-1-i]['status'] is None else response['kbLinks'][document_kbLinks_len-1-i]['status']:
                    tmp = [i]
                    tmp.append("status")
                    tmp.append(document['link'][i]['status'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['status'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['description'] == response['kbLinks'][document_kbLinks_len-1-i]['description']:
                    tmp = [i]
                    tmp.append("description")
                    tmp.append(document['link'][i]['description'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['description'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['internalId'] == response['kbLinks'][document_kbLinks_len-1-i]['internalId']:
                    tmp = [i]
                    tmp.append("internalId")
                    tmp.append(document['link'][i]['internalId'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['internalId'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['url'] == response['kbLinks'][document_kbLinks_len-1-i]['url']:
                    tmp = [i]
                    tmp.append("url")
                    tmp.append(document['link'][i]['url'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['url'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['kbDate'] == response['kbLinks'][document_kbLinks_len-1-i]['kbDate']:
                    tmp = [i]
                    tmp.append("kbDate")
                    tmp.append(document['link'][i]['kbDate'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['kbDate'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['sourceVisibility'] == "" if response['kbLinks'][document_kbLinks_len-1-i]['srcVisiblity'] is None else response['kbLinks'][document_kbLinks_len-1-i]['srcVisiblity']:
                    tmp = [i]
                    tmp.append("sourceVisibility/srcVisiblity")
                    tmp.append(document['link'][i]['sourceVisibility'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['srcVisiblity'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['integrated'] == response['kbLinks'][document_kbLinks_len-1-i]['integrated']:
                    tmp = [i]
                    tmp.append("integrated")
                    tmp.append(document['link'][i]['integrated'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['integrated'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1

                if not document['link'][i]['srVisibility'] == response['kbLinks'][document_kbLinks_len-1-i]['srVisibility']:
                    tmp = [i]
                    tmp.append("srVisibility")
                    tmp.append(document['link'][i]['srVisibility'])
                    tmp.append(response['kbLinks'][document_kbLinks_len-1-i]['srVisibility'])
                    tmp.append("Failed")
                    table.append(tmp)
                    status = 1
                tmp = []
            if status == 0:
                print "Match Found"
                row.append("Match Found")
            else:
                print tabulate(table, headers=["LinkNo", "Key", "Kafka", "API", "Status"], tablefmt="rst")

        else:
            print "No Match Found"
            row.append("No Match Found")'''
        return row

    def get_case_note_desc_by_case_note_id(self, case_id, note_id):
        pass

    def get_case_note_log_by_case_note_id(self, case_id, note_id):
        pass

    def get_account_by_account_id(self, account_id):
        pass

    def get_account_by_account_name(self, account_name):
        pass

    def get_user_by_user_id(self, user_id):
        pass

    def get_user_by_user_name(self, user_name):
        pass


#client = MongoClient('10.219.48.134', 27017)
client = MongoClient('10.219.48.134', 27017)
db = client['SAPEvent']
collection = db['customerMaster']
api = HBase()
document_no = 0
documents = collection.find( {} )
#documents = collection.find( {'caseId':'2015-1008-T-0003'} )
ofile = open('customerMaster.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
row = ["SNo", "CaseID", "KafkaJSON", "APIResponse", "Status"]
writer.writerow(row)
for document in documents:
    row = []
    document_no += 1
    row.append(document_no)
    #row.append(document['caseId'])
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

