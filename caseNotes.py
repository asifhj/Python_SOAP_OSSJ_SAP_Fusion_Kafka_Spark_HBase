__author__ = 'asifj'
import requests
from pymongo import MongoClient
import json
import csv
import traceback
import logging
from tabulate import tabulate
from bson.json_util import dumps

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

class HBase:
    def __init__(self):
        self.url = "http://172.22.147.248:8092/api/"
        pass

    def get_case_by_case_id(self, document, row):
        print "Making request..."
        print self.url+"case-manager/cases/"+str(document['caseId'])
        r = requests.get(self.url+"case-manager/cases/"+str(document['caseId']))
        print "CaseID: "+str(document['caseId'])
        print "Response: "+str(r.status_code)
        keys = len(document.keys())
        print "Keys: "+str(keys)
        row.append(r.status_code)
        if r.status_code==200:
            response = json.loads(r.text)
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                print "Incorrect value for 'caseId'!"
                status = 1
            print "Document"
            print "========"
            print dumps(document, sort_keys=True)
            print "Response CaseNotes"
            print "=================="
            print dumps(response['caseNotes'][4], sort_keys=True)

            response_notes_len = len(response['caseNotes'])
            document_notes_len = len(document['note'])

            print "No of notes in response: "+str(response_notes_len)
            print "No of notes in document: "+str(document_notes_len)
            found = 0
            for i in range(0, response_notes_len):
                tmp = []
                if document['note']['tdline'] == response['caseNotes'][i]['noteLogMin']:
                    found == 1
                    changeDate = "" if response['caseNotes'][i]['changeDate'] is None else str(response['caseNotes'][i]['changeDate']).strip()
                    changeTime = "" if response['caseNotes'][i]['changeTime'] is None else str(response['caseNotes'][i]['changeTime']).strip()
                    originatorRole = "" if response['caseNotes'][i]['originatorRole'] is None else str(response['caseNotes'][i]['originatorRole']).strip()
                    responsibleGroup = "" if response['caseNotes'][i]['responsibleGroup'] is None else str(response['caseNotes'][i]['responsibleGroup']).strip()
                    countryKey = "" if response['caseNotes'][i]['countryKey'] is None else str(response['caseNotes'][i]['countryKey']).strip()
                    noteType = "" if response['caseNotes'][i]['noteType'] is None else str(response['caseNotes'][i]['noteType']).strip()
                    creationMethod = "" if response['caseNotes'][i]['creationMethod'] is None else str(response['caseNotes'][i]['creationMethod']).strip()
                    originator = "" if response['caseNotes'][i]['originator'] is None else str(response['caseNotes'][i]['originator']).strip()
                    privatePublic = "" if response['caseNotes'][i]['privatePublic'] is None else str(response['caseNotes'][i]['privatePublic']).strip()
                    supervisor = "" if response['caseNotes'][i]['supervisor'] is None else str(response['caseNotes'][i]['supervisor']).strip()
                    theater = "" if response['caseNotes'][i]['theater'] is None else str(response['caseNotes'][i]['theater']).strip()
                    noteName = "" if response['caseNotes'][i]['noteName'] is None else str(response['caseNotes'][i]['noteName']).strip()
                    changeDate = "" if response['caseNotes'][i]['changeDate'] is None else str(response['caseNotes'][i]['changeDate']).strip()

                    if not str(document['udate']).strip() == changeDate:
                        tmp = [i]
                        tmp.append("udate")
                        tmp.append(document['udate'])
                        tmp.append(response['caseNotes'][i]['changeDate'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['utime']).strip() == changeTime:
                        tmp = [i]
                        tmp.append("utime")
                        tmp.append(document['utime'])
                        tmp.append(response['caseNotes'][i]['changeTime'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zoriginatorrole']).strip() == originatorRole:
                        tmp = [i]
                        tmp.append("zoriginatorrole")
                        tmp.append(document['zoriginatorrole'])
                        tmp.append(response['caseNotes'][i]['originatorRole'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zrespgroup']).strip() == responsibleGroup:
                        tmp = [i]
                        tmp.append("zrespgroup")
                        tmp.append(document['zrespgroup'])
                        tmp.append(response['caseNotes'][i]['responsibleGroup'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zcountry']).strip() == countryKey:
                        tmp = [i]
                        tmp.append("zcountry")
                        tmp.append(document['zcountry'])
                        tmp.append(response['caseNotes'][i]['countryKey'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['tdid']).strip() == noteType:
                        tmp = [i]
                        tmp.append("tdid")
                        tmp.append(document['tdid'])
                        tmp.append(response['caseNotes'][i]['noteType'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zmethod']).strip() == creationMethod:
                        tmp = [i]
                        tmp.append("zmethod")
                        tmp.append(document['zmethod'])
                        tmp.append(response['caseNotes'][i]['creationMethod'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zorignator']).strip() == originator:
                        tmp = [i]
                        tmp.append("zorignator")
                        tmp.append(document['zorignator'])
                        tmp.append(response['caseNotes'][i]['originator'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zpublic']).strip() == privatePublic:
                        tmp = [i]
                        tmp.append("zpublic")
                        tmp.append(document['zpublic'])
                        tmp.append(response['caseNotes'][i]['privatePublic'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['zsupervisor']).strip() == supervisor:
                        tmp = [i]
                        tmp.append("zsupervisor")
                        tmp.append(document['zsupervisor'])
                        tmp.append(response['caseNotes'][i]['supervisor'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['ztheater']).strip() == theater:
                        tmp = [i]
                        tmp.append("ztheater")
                        tmp.append(document['ztheater'])
                        tmp.append(response['caseNotes'][i]['theater'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
                    if not str(document['tdname']).strip() == noteName:
                        tmp = [i]
                        tmp.append("tdname")
                        tmp.append(document['tdname'])
                        tmp.append(response['caseNotes'][i]['noteName'])
                        tmp.append("Failed")
                        table.append(tmp)
                        found = 1
            if found == 1:
                print "Match Found"
                row.append("Match Found")
                print tabulate(table, headers=["NoteNo", "Key", "Kafka", "API", "Status"], tablefmt="rst")
            else:
                print tabulate(table, headers=["NoteNo", "Key", "Kafka", "API", "Status"], tablefmt="rst")

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
db = client['SAPNotesTopic']
collection = db['caseNotes']
api = HBase()
document_no = 0
documents = collection.find({})
#documents = collection.find({'caseId':'2015-0924-T-2500'})
print {'caseId': '2015-0924-T-2500'}
ofile = open('caseNotes.csv', "wb")
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

