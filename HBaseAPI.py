__author__ = 'asifj'
import requests
from pymongo import MongoClient
from bson import Binary, Code
import json
import csv

class HBase:
    def __init__(self):
        self.url = "http://172.22.147.248:8092/api/"
        pass

    def get_case_by_case_id(self, document, row):
        print self.url+"case-manager/cases/"+str(document['caseId'])
        r = requests.get(self.url+"case-manager/cases/"+str(document['caseId']))
        print str(document['caseId'])
        print r.status_code
        row.append(r.status_code)
        if r.status_code==200:
            response = json.loads(r.text)
            if not str(document['betaType']).strip().strip()=="" if response['betaType'] is None else response['betaType'] :
                print "Not Found"
            if not str(document['caseId']).strip()=="" if response['srId']  is None else response['srId']:
                print "Not Found"
            if not str(document['contractId']).strip()=="" if response['entitlement']['contractId'] is None else response['entitlement']['contractId']:
                print "Not Found"
            if not str(document['contractStatus']).strip()=="" if response['entitlement']['contractStatus'] is None else response['entitlement']['contractStatus']:
                print "Not Found"
            if not str(document['courtesykey']).strip()=="" if response['courtesyKey'] is None else response['courtesyKey']:
                print "Not Found"
            if not str(document['criticalOutage']).strip()=="" if response['criticalOutage'] is None else response['criticalOutage']:
                print "Not Found"
            if not str(document['cve']).strip()=="" if response['cve'] is None else response['cve']:
                print "Not Found"
            if not str(document['cvss']).strip()=="" if response['cvss'] is None else response['cvss']:
                print "Not Found"
            if not str(document['description']).strip()=="" if response['desc'] is None else response['desc']:
                print "Not Found"
            if not str(document['endDate']).strip()=="" if response['entitlement']['endDate'] is None else response['entitlement']['endDate']:
                print "Not Found"
            if not str(document['entitledSerialNumber']).strip()=="" if response['entitlement']['entitledSerialNumber'] is None else response['entitlement']['entitledSerialNumber']:
                print "Not Found"
            if not str(document['entitlementChecked']).strip()=="" if response['entitlement']['entitlementChecked'] is None else response['entitlement']['entitlementChecked']:
                print "Not Found"
            if not str(document['entitlementServiceLevel']).strip()=="" if response['entitlement']['entitlementServiceLevel'] is None else response['entitlement']['entitlementServiceLevel']:
                print "Not Found"
            if not str(document['entitlementSource']).strip()=="" if response['entitlement']['entitlementSource'] is None else response['entitlement']['entitlementSource']:
                print "Not Found"
            if not str(document['escalationLevelKey']).strip()=="" if response['outage']['escalationLevelkey'] is None else response['outage']['escalationLevelkey']:
                print "Not Found"
            if not str(document['escalationkey']).strip()=="" if response['escalationKey'] is None else response['escalationKey']:
                print "Not Found"
            if not str(document['externallyReported']).strip()=="" if response['externallyReported'] is None else response['externallyReported']:
                print "Not Found"
            if not str(document['followupMethod']).strip()=="" if response['outage']['followUpMethod'] is None else response['outage']['followUpMethod']:
                print "Not Found"
            if not str(document['followupMethodKey']).strip()=="" if response['outage']['followUpMethodkey'] is None else response['outage']['followUpMethodkey']:
                print "Not Found"
            if not str(document['jsaAdvisoryBoard']).strip()=="" if response['jsaAdvisoryBoard'] is None else response['jsaAdvisoryBoard']:
                print "Not Found"
            if not str(document['jtac']).strip()=="" if response['jtac'] is None else response['jtac']:
                print "Not Found"
            if not str(document['knowledgeArticle']).strip()=="" if response['outage']['knowledgeArticle'] is None else response['outage']['knowledgeArticle']:
                print "Not Found"
            if not str(document['outageCauseKey']).strip()=="" if response['outage']['outageCausekey'] is None else response['outage']['outageCausekey']:
                print "Not Found"
            if not str(document['outageImpactKey']).strip()=="" if response['outage']['outageImpactKey'] is None else response['outage']['outageImpactKey']:
                print "Not Found"
            if not str(document['outageInfoAvailable']).strip()=="" if response['outage']['outageInfoAvailable'] is None else response['outage']['outageInfoAvailable']:
                print "Not Found"
            if not str(document['outageKey']).strip()=="" if response['outage']['outageKey'] is None else response['outage']['outageKey']:
                print "Not Found"
            if not str(document['outageTypeKey']).strip()=="" if response['outage']['outageTypekey'] is None else response['outage']['outageTypekey']:
                print "Not Found"
            if not str(document['outsourcer']).strip()=="" if response['outage']['outsourcer'] is None else response['outage']['outsourcer']:
                print "Not Found"
            if not str(document['overideOutage']).strip()=="" if response['outage']['overideOutage'] is None else response['outage']['overideOutage']:
                print "Not Found"
            if not str(document['platform']).strip()=="" if response['platform'] is None else response['platform']:
                print "Not Found"
            if not str(document['previousOwnerSkill']).strip()=="" if response['outage']['previousOwnerSkill'] is None else response['outage']['previousOwnerSkill']:
                print "Not Found"
            if not str(document['previousTeam']).strip()=="" if response['outage']['previousTeam'] is None else response['outage']['previousTeam']:
                print "Not Found"
            if not str(document['priority']).strip()=="" if response['priority'] is None else response['priority']:
                print "Not Found"
            if not str(document['priorityKey']).strip()=="" if response['priorityKey'] is None else response['priorityKey']:
                print "Not Found"
            if not str(document['processType']).strip()=="" if response['processType'] is None else response['processType']:
                print "Not Found"
            if not str(document['processTypeDescription']).strip()=="" if response['processTypeDesc'] is None else response['processTypeDesc']:
                print "Not Found"
            if not str(document['productId']).strip()=="" if response['productId'] is None else response['productId']:
                print "Not Found"
            if not str(document['productSeries']).strip()=="" if response['productSeries'] is None else response['productSeries']:
                print "Not Found"
            if not str(document['raFa']).strip()=="" if response['outage']['raFa'] is None else response['outage']['raFa']:
                print "Not Found"
            if not str(document['reason']).strip()=="" if response['reason'] is None else response['reason']:
                print "Not Found"
            if not str(document['release']).strip()=="" if response['release'] is None else response['release']:
                print "Not Found"
            if not str(document['reporterDetails']).strip()=="" if response['reporterDetails'] is None else response['reporterDetails']:
                print "Not Found"
            if not str(document['routerName']).strip()=="" if response['outage']['routerName'] is None else response['outage']['routerName']:
                print "Not Found"
            if not str(document['secVulnerability']).strip()=="" if response['secVulnerability'] is None else response['secVulnerability']:
                print "Not Found"
            if not str(document['serialNumber']).strip()=="" if response['serialNumber'] is None else response['serialNumber']:
                print "Not Found"
            if not str(document['severity']).strip()=="" if response['severity'] is None else response['severity']:
                print "Not Found"
            if not str(document['severityKey']).strip()=="" if response['severityKey'] is None else response['severityKey']:
                print "Not Found"
            if not str(document['sirtBundle']).strip()=="" if response['sirtBundle'] is None else response['sirtBundle']:
                print "Not Found"
            if not str(document['sku']).strip()=="" if response['entitlement']['sku'] is None else response['entitlement']['sku']:
                print "Not Found"
            if not str(document['smeContact']).strip()=="" if response['smeContact'] is None else response['smeContact']:
                print "Not Found"
            if not str(document['software']).strip()=="" if response['software'] is None else response['software']:
                print "Not Found"
            if not str(document['specialRelease']).strip()=="" if response['specialRelease'] is None else response['specialRelease']:
                print "Not Found"
            if not str(document['srCategory1']).strip()=="" if response['srCat1'] is None else response['srCat1']:
                print "Not Found"
            if not str(document['srCategory2']).strip()=="" if response['srCat2'] is None else response['srCat2']:
                print "Not Found"
            if not str(document['srCategory3']).strip()=="" if response['srCat3'] is None else response['srCat3']:
                print "Not Found"
            if not str(document['srCategory4']).strip()=="" if response['srCat4'] is None else response['srCat4']:
                print "Not Found"
            if not str(document['startDate']).strip()=="" if response['entitlement']['startDate'] is None else response['entitlement']['startDate']:
                print "Not Found"
            if not str(document['status']).strip()=="" if response['status'] is None else response['status']:
                print "Not Found"
            if not str(document['statusKey']).strip()=="" if response['statusKey'] is None else response['statusKey']:
                print "Not Found"
            if not str(document['technicalCategory1']).strip()=="" if response['techCat1'] is None else response['techCat1']:
                print "Not Found"
            if not str(document['technicalCategory2']).strip()=="" if response['techCat2'] is None else response['techCat2']:
                print "Not Found"
            if not str(document['technicalCategory3']).strip()=="" if response['techCat3'] is None else response['techCat3']:
                print "Not Found"
            if not str(document['technicalCategory4']).strip()=="" if response['techCat4'] is None else response['techCat4']:
                print "Not Found"
            if not str(document['temperature']).strip()=="" if response['outage']['temperature'] is None else response['outage']['temperature']:
                print "Not Found"
            if not str(document['theaterKey']).strip()=="" if response['outage']['theaterKey'] is None else response['outage']['theaterKey']:
                print "Not Found"
            if not str(document['top5']).strip()=="" if response['outage']['top5'] is None else response['outage']['top5']:
                print "Not Found"
            if not str(document['totalOutageTime']).strip()=="" if response['outage']['totalOutageTime'] is None else response['outage']['totalOutageTime']:
                print "Not Found"
            if not str(document['urgency']).strip()=="" if response['urgency'] is None else response['urgency']:
                print "Not Found"
            if not str(document['urgencyKey']).strip()=="" if response['urgencyKey'] is None else response['urgencyKey']:
                print "Not Found"
            if not str(document['version']).strip()=="" if response['version'] is None else response['version']:
                print "Not Found"
            if not str(document['viaKey']).strip()=="" if response['outage']['viaKey'] is None else response['outage']['viaKey']:
                print "Not Found"
            if not str(document['warrantyEndDate']).strip()=="" if response['entitlement']['warrantyEndDate'] is None else response['entitlement']['warrantyEndDate']:
                print "Not Found"
                print "Match Found"
                row.append("Match Found")
            else:
                print "Not Found"
                row.append("Not Found")
        else:
            print "Not Found"
            row.append("Not Found")
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
db = client['SAPEvent']
collection = db['srDetails']
api = HBase()
document_no = 0
#documents = collection.find({'caseId': '2015-1016-T-0017'})
documents = collection.find({})
ofile  = open('HBaseAPIOutput.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
row = ["SNo", "CaseID", "KafkaJSON", "APIResponse", "Status"]
writer.writerow(row)
for document in documents:
    row = []
    document_no += 1
    row.append(document_no)
    row.append(document['caseId'])
    row.append(str(document).replace("\n", ""))
    print "++++++++++++++++"
    print "Document No: "+str(document_no)
    row = api.get_case_by_case_id(document, row)
    writer.writerow(row)
    print "\n\n"
ofile.close()

