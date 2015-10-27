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
        print self.url+"case-manager/cases/"+str(document['caseId'])
        r = requests.get(self.url+"case-manager/cases/"+str(document['caseId']))
        print "CaseID: "+str(document['caseId'])
        print "Response: "+str(r.status_code)
        keys = len(document.keys())
        print "Keys: "+str(keys)
        row.append(r.status_code)
        status = 0
        if r.status_code==200:
            response = json.loads(r.text)
            #print json.dumps(response, indent=4)
            #print document
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                print "Incorrect value for 'caseId'!"
                status = 1

            response_attachment_len = len(response['attachments'])
            document_attachment_len = len(document['attachment'])
            #print json.dumps(document['attachment'], indent=4)
            #print json.dumps(response['attachments'], indent=4)
            if response_attachment_len>=document_attachment_len:
                for i in range(0, document_attachment_len):
                    tmp = [i]
                    #print json.dumps(document['attachment'][i], indent=4)
                    #print json.dumps(response['attachments'][i], indent=4)
                    zDate = str(document['attachment'][i]['zDate'])
                    zDate = zDate[:4]+"-"+zDate[4:6]+"-"+zDate[6:]
                    if not document['attachment'][i]['sequenceNumber'] == response['attachments'][i]['attNo']:
                        tmp.append("sequenceNumber")
                        tmp.append(document['attachment'][i]['sequenceNumber'])
                        tmp.append(response['attachments'][i]['attNo'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]

                    if not document['attachment'][i]['title'] == response['attachments'][i]['title']:
                        tmp.append("title")
                        tmp.append(document['attachment'][i]['title'])
                        tmp.append(response['attachments'][i]['title'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['zTime'] == response['attachments'][i]['zTime']:
                        tmp.append("zTime")
                        tmp.append(document['attachment'][i]['zTime'])
                        tmp.append(response['attachments'][i]['zTime'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['fileType'] == response['attachments'][i]['fileType']:
                        tmp.append("fileType")
                        tmp.append(document['attachment'][i]['fileType'])
                        tmp.append(response['attachments'][i]['fileType'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['private'] == response['attachments'][i]['private1']:
                        tmp.append("private")
                        tmp.append(document['attachment'][i]['private'])
                        tmp.append(response['attachments'][i]['private1'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['dateCreated'] == response['attachments'][i]['dateCreated']:
                        tmp.append("dateCreated")
                        tmp.append(document['attachment'][i]['dateCreated'])
                        tmp.append(response['attachments'][i]['dateCreated'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['createdBy'] == response['attachments'][i]['createdBy']:
                        tmp.append("createdBy")
                        tmp.append(document['attachment'][i]['createdBy'])
                        tmp.append(response['attachments'][i]['createdBy'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['path'] == response['attachments'][i]['path']:
                        tmp.append("path")
                        tmp.append(document['attachment'][i]['path'])
                        tmp.append(response['attachments'][i]['path'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['uploadedBy'] == response['attachments'][i]['uploadedBy']:
                        tmp.append("uploadedBy")
                        tmp.append(document['attachment'][i]['uploadedBy'])
                        tmp.append(response['attachments'][i]['uploadedBy'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not document['attachment'][i]['size'] == response['attachments'][i]['size1']:
                        tmp.append("size")
                        tmp.append(document['attachment'][i]['size'])
                        tmp.append(response['attachments'][i]['size1'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1

                    if not zDate == response['attachments'][i]['zDate']:
                        tmp.append("zDate")
                        tmp.append(zDate)
                        tmp.append(response['attachments'][i]['zDate'])
                        tmp.append("Failed")
                        table.append(tmp)
                        tmp = [i]
                        status = 1
                if status == 0:
                    print "Match Found"
                    row.append("Match Found")
                else:
                    print tabulate(table, headers=["AttachmentNo", "Key", "Kafka", "API", "Status"], tablefmt="rst")
        else:
            print "No Match Found"
            row.append("No Match Found")
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


client = MongoClient('10.219.48.134', 27017)
#client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['srAttachements']
api = HBase()
document_no = 0
documents = collection.find({})
#documents = collection.find({'caseId':'2015-1005-T-0028'})
ofile = open('srAttachments.csv', "wb")
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
        #print json.dumps(document, indent=4)
        row = api.get_case_by_case_id(document, row)
    except Exception:
        print Exception.message
        print(traceback.format_exc())
    writer.writerow(row)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "\n\n"
ofile.close()

