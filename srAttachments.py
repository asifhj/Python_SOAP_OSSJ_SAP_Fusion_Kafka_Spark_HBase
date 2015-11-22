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
            if response['attachments'] is not None:
                response_attachment_len = len(response['attachments'])
            else:
                response_attachment_len = 0
            document_attachment_len = len(document['attachment'])
            if type(document['attachment']) is dict:
                print "attachment in document is not an array!"
                document_attachment_len = 1
                document['attachment'] =  [document['attachment']]

            print "Number of attachments in document: "+str(document_attachment_len)
            print "Number of attachments in API response: "+str(response_attachment_len)

            if document_attachment_len==0:
                print "No attachment found in document!"
                row.append("No attachment found in document!")
                print "Kafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                print "API: "+str(json.dumps(response['attachments'], sort_keys=True))
                return row
            if response_attachment_len==0 and document_attachment_len>0:
                print "No attachment found in API response but present in document."
                row.append("No attachment found in API response but present in document.")
                print "Kafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                print "API: "+str(json.dumps(response['attachments'], sort_keys=True))
                return row


            for doc_attachment in document['attachment']:
                match_level = 0
                found = 0
                match_location = 0
                counter = 0
                old_match_level = 0
                zDate = str(doc_attachment['zDate'])
                zDate = zDate[:4]+"-"+zDate[4:6]+"-"+zDate[6:]
                match_data = ""
                for resp in response['attachments']:
                    match_level = 0
                    doc_private = doc_attachment.get('private', "")
                    if doc_private:
                        doc_private = doc_attachment.get('Private', "")
                    if doc_private:
                        doc_private = doc_attachment.get('isPrivate', "")
                    if doc_attachment['sequenceNumber'] == ("" if resp['attNo'] is None else resp['attNo']):
                        match_level += 1
                    if doc_attachment['title'] == ("" if resp['title'] is None else resp['title']):
                        match_level += 1
                    if doc_attachment['zTime'] == ("" if resp['zTime'] is None else resp['zTime']):
                        match_level += 1
                    if doc_attachment['fileType'] == ("" if resp['fileType'] is None else resp['fileType']):
                        match_level += 1
                    if doc_private == ("" if resp['private1'] is None else resp['private1']):
                        match_level += 1
                    if doc_attachment['dateCreated'] == ("" if resp['dateCreated'] is None else resp['dateCreated']):
                        match_level += 1
                    if doc_attachment['createdBy'] == ("" if resp['createdBy'] is None else resp['createdBy']):
                        match_level += 1
                    if doc_attachment['path'] == ("" if resp['path'] is None else resp['path']):
                        match_level += 1
                    if doc_attachment['uploadedBy'] == ("" if resp['uploadedBy'] is None else resp['uploadedBy']):
                        match_level += 1
                    if doc_attachment['size'] == ("" if resp['size1'] is None else int(resp['size1'])):
                        match_level += 1
                    if zDate == ("" if resp['zDate'] is None else resp['zDate']):
                        if match_level >= 10:
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
                    print "Kafka ==> "+str(json.dumps(doc_attachment, sort_keys=True))
                    print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                    print set(doc_attachment) ^ set(match_data)
                    tmp = ["", ""]
                    tmp.append("Incorrect value for 'attachment'!")
                    table.append(tmp)
                    status = 1
                    print "************************************************"
                else:
                    print "Data matched, highest level of match is "+str(match_level)
                    print "Kafka ==> "+str(json.dumps(doc_attachment, sort_keys=True))
                    print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", ""]
                    tmp.append("Match found for 'attachment'!")
                    table.append(tmp)
            if status == 0:
                print "Match Found"
                row.append("Match Found")
            else:
                print "\nCompared JSONs"
                print "Kafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                print "API: "+str(json.dumps(response['attachments'], sort_keys=True))
                print tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            print "No Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        return row



client = MongoClient('10.219.48.134', 27017)
#client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['srAttachements']
api = HBase()
document_no = 0
#documents = collection.find({})
#documents = collection.find(no_cursor_timeout=True)[0:]
documents = collection.find({ 'caseId': '2015-1116-T-0002'})
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
        print "Kafka: "+str(document)
        print Exception.message
        print(traceback.format_exc())
    writer.writerow(row)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "\n\n"
ofile.close()

