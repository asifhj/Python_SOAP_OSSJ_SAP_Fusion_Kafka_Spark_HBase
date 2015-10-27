__author__ = 'asifj'
import requests
from pymongo import MongoClient
from bson import Binary, Code
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
            table = []
            if not (str(document['betaType']).strip() == "" if response['betaType'] is None else str(response['betaType']).strip()):
                tmp = [str(document['betaType']).strip(), str(response['betaType']).strip()]
                tmp.append("Incorrect value for 'betaType'!")
                table.append(tmp)
                status = 1

            if not (str(document['build']).strip() == "" if response['outage']['build'] is None else str(response['outage']['build']).strip()):
                tmp = [str(document['build']).strip(), str(response['outage']['build']).strip()]
                tmp.append("Incorrect value for 'build'!")
                table.append(tmp)
                status = 1

            if not (str(document['ccEngineer']).strip() == "" if response['outage']['ccEngineer'] is None else str(response['outage']['ccEngineer']).strip()):
                tmp = [str(document['ccEngineer']).strip(), str(response['outage']['ccEngineer']).strip()]
                tmp.append("Incorrect value for 'ccEngineer'!")
                table.append(tmp)
                status = 1

            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                tmp = [str(document['caseId']).strip(), str(response['srId']).strip()]
                tmp.append("Incorrect value for 'caseId'!")
                table.append(tmp)
                status = 1

            if not (str(document['contractId']).strip() == "" if response['entitlement']['contractId'] is None else str(response['entitlement']['contractId']).strip()):
                tmp = [str(document['contractId']).strip(), str(response['entitlement']['contractId']).strip()]
                tmp.append("Incorrect value for 'contractId'!")
                table.append(tmp)
                status = 1

            if not (str(document['contractStatus']).strip()=="" if response['entitlement']['contractStatus'] is None else str(response['entitlement']['contractStatus']).strip()):
                tmp = [str(document['contractStatus']).strip(), str(response['entitlement']['contractStatus']).strip()]
                tmp.append("Incorrect value for 'contractStatus'!")
                table.append(tmp)
                status = 1

            if not (str(document['country']).strip() == "" if response['outage']['country'] is None else str(response['outage']['country']).strip()):
                tmp = [str(document['country']).strip(), str(response['outage']['country']).strip()]
                tmp.append("Incorrect value for 'country'!")
                table.append(tmp)
                status = 1

            if not (str(document['courtesyDescription']).strip()=="" if response['courtesy'] is None else str(response['courtesy']).strip()):
                tmp = [str(document['courtesyDescription']).strip(), str(response['courtesy']).strip()]
                tmp.append("Incorrect value for 'courtesyDescription/courtesy'!")
                table.append(tmp)
                status = 1

            if not (str(document['courtesykey']).strip()=="" if response['courtesyKey'] is None else str(response['courtesyKey']).strip()):
                tmp = [str(document['courtesykey']).strip(), str(response['courtesyKey']).strip()]
                tmp.append("Incorrect value for 'courtesykey'!")
                table.append(tmp)
                status = 1

            if not (str(document['criticalIssue']).strip()=="" if response['outage']['criticalIssue'] is None else str(response['outage']['criticalIssue']).strip()):
                tmp = [str(document['criticalIssue']).strip(), str(response['outage']['criticalIssue']).strip()]
                tmp.append("Incorrect value for 'criticalIssue'!")
                table.append(tmp)
                status = 1

            if not (str(document['criticalOutage']).strip()=="" if response['criticalOutage'] is None else str(response['criticalOutage']).strip()):
                tmp = [str(document['criticalOutage']).strip(), str(response['criticalOutage']).strip()]
                tmp.append("Incorrect value for 'criticalOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['customerCaseNumber']).strip()=="" if response['outage']['custCaseNo'] is None else str(response['outage']['custCaseNo']).strip()):
                tmp = [str(document['customerCaseNumber']).strip(), str(response['outage']['custCaseNo']).strip()]
                tmp.append("Incorrect value for 'customerCaseNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['cve']).strip()=="" if response['cve'] is None else str(response['cve']).strip()):
                tmp = [str(document['cve']).strip(), str(response['cve']).strip()]
                tmp.append("Incorrect value for 'cve'!")
                table.append(tmp)
                status = 1

            if not (str(document['cvss']).strip()=="" if response['cvss'] is None else str(response['cvss']).strip()):
                tmp = [str(document['cvss']).strip(), str(response['cvss']).strip()]
                tmp.append("Incorrect value for 'cvss'!")
                table.append(tmp)
                status = 1

            if not (str(document['description']).strip()=="" if response['desc'] is None else str(response['desc']).strip()):
                tmp = [str(document['description']).strip(), str(response['desc']).strip()]
                tmp.append("Incorrect value for 'description'!")
                table.append(tmp)
                status = 1

            if not (str(document['endDate']).strip() == "" if response['entitlement']['endDate'] is None else str(response['entitlement']['endDate']).strip()):
                tmp = [str(document['endDate']).strip(), str(response['entitlement']['endDate']).strip()]
                tmp.append("Incorrect value for 'endDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitledSerialNumber']).strip()=="" if response['entitlement']['entitledSerialNumber'] is None else str(response['entitlement']['entitledSerialNumber']).strip()):
                tmp = [str(document['entitledSerialNumber']).strip(), str(response['entitlement']['entitledSerialNumber']).strip()]
                tmp.append("Incorrect value for 'entitledSerialNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementChecked']).strip()=="" if response['entitlement']['entitlementChecked'] is None else str(response['entitlement']['entitlementChecked']).strip()):
                tmp = [str(document['entitlementChecked']).strip(), str(response['entitlement']['entitlementChecked']).strip()]
                tmp.append("Incorrect value for 'entitlementChecked'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementServiceLevel']).strip()=="" if response['entitlement']['entitlementServiceLevel'] is None else str(response['entitlement']['entitlementServiceLevel']).strip()):
                tmp = [str(document['entitlementServiceLevel']).strip(), str(response['entitlement']['entitlementServiceLevel']).strip()]
                tmp.append("Incorrect value for 'entitlementServiceLevel'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementSource']).strip()=="" if response['entitlement']['entitlementSource'] is None else str(response['entitlement']['entitlementSource']).strip()):
                tmp = [str(document['entitlementSource']).strip(), str(response['entitlement']['entitlementSource']).strip()]
                tmp.append("Incorrect value for 'entitlementSource'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('escalation', '')).strip()=="" if response['escalationDesc'] is None else str(response['escalationDesc']).strip()):
                tmp = [str(document['escalation']).strip(), str(response['escalationDesc']).strip()]
                tmp.append("Incorrect value for 'escalation'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationLevelDescription']).strip()=="" if response['outage']['escalationLevel'] is None else str(response['outage']['escalationLevel']).strip()):
                tmp = [str(document['escalationLevelDescription']).strip(), str(response['outage']['escalationLevel']).strip()]
                tmp.append("Incorrect value for 'escalationLevelDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationLevelKey']).strip()=="" if response['outage']['escalationLevelkey'] is None else str(response['outage']['escalationLevelkey']).strip()):
                tmp = [str(document['escalationLevelKey']).strip(), str(response['outage']['escalationLevelkey']).strip()]
                tmp.append("Incorrect value for 'escalationLevelKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationkey']).strip()=="" if response['escalationKey'] is None else str(response['escalationKey']).strip()):
                tmp = [str(document['escalationkey']).strip(), str(response['escalationKey']).strip()]
                tmp.append("Incorrect value for 'escalationkey'!")
                table.append(tmp)
                status = 1

            if not (str(document['externallyReported']).strip()=="" if response['externallyReported'] is None else str(response['externallyReported']).strip()):
                tmp = [str(document['externallyReported']).strip(), str(response['externallyReported']).strip()]
                tmp.append("Incorrect value for 'externallyReported'!")
                table.append(tmp)
                status = 1

            #if  keys>100:
            if not (str(document.get('followupMethod', '')).strip()=="" if response['outage']['followUpMethod'] is None else str(response['outage']['followUpMethod']).strip()):
                tmp = [str(document['followupMethod']).strip(), str(response['outage']['followUpMethod']).strip()]
                tmp.append("Incorrect value for 'followupMethod'!")
                table.append(tmp)
                status = 1

            if not (str(document['followupMethodKey']).strip()=="" if response['outage']['followUpMethodkey'] is None else str(response['outage']['followUpMethodkey']).strip()):
                tmp = [str(document['followupMethodKey']).strip(), str(response['outage']['followUpMethodkey']).strip()]
                tmp.append("Incorrect value for 'followupMethodKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['jsaAdvisoryBoard']).strip()=="" if response['jsaAdvisoryBoard'] is None else str(response['jsaAdvisoryBoard']).strip()):
                tmp = [str(document['jsaAdvisoryBoard']).strip(), str(response['jsaAdvisoryBoard']).strip()]
                tmp.append("Incorrect value for 'jsaAdvisoryBoard'!")
                table.append(tmp)
                status = 1

            if not (str(document['jtac']).strip()=="" if response['jtac'] is None else str(response['jtac']).strip()):
                tmp = [str(document['jtac']).strip(), str(response['jtac']).strip()]
                tmp.append("Incorrect value for 'jtac'!")
                table.append(tmp)
                status = 1

            if not (str(document['knowledgeArticle']).strip()=="" if response['outage']['knowledgeArticle'] is None else str(response['outage']['knowledgeArticle']).strip()):
                tmp = [str(document['knowledgeArticle']).strip(), str(response['outage']['knowledgeArticle']).strip()]
                tmp.append("Incorrect value for 'knowledgeArticle'!")
                table.append(tmp)
                status = 1

            '''if not (str(document['outageCauseDescription']).strip()=="" if response['outage']['ouatgeCause'] is None else str(response['outage']['ouatgeCause']).strip()):
                tmp = [str(document['outageCauseDescription']).strip(), str(response['outage']['ouatgeCause']).strip()]
                tmp.append("Incorrect value for 'outageCauseDescription/ouatgeCause'!")
                table.append(tmp)
                status = 1'''

            if not (str(document['outageCauseKey']).strip()=="" if response['outage']['outageCausekey'] is None else str(response['outage']['outageCausekey']).strip()):
                tmp = [str(document['outageCauseKey']).strip(), str(response['outage']['outageCausekey']).strip()]
                tmp.append("Incorrect value for 'outageCauseKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageDescription']).strip()=="" if response['outage']['outage'] is None else str(response['outage']['outage']).strip()):
                tmp = [str(document['outageDescription']).strip(), str(response['outage']['outage']).strip()]
                tmp.append("Incorrect value for 'outageDescription/outage'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageImpactKey']).strip()=="" if response['outage']['outageImpactKey'] is None else str(response['outage']['outageImpactKey']).strip()):
                tmp = [str(document['outageImpactKey']).strip(), str(response['outage']['outageImpactKey']).strip()]
                tmp.append("Incorrect value for 'outageImpactKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageInfoAvailable']).strip()=="" if response['outage']['outageInfoAvailable'] is None else str(response['outage']['outageInfoAvailable']).strip()):
                tmp = [str(document['outageInfoAvailable']).strip(), str(response['outage']['outageInfoAvailable']).strip()]
                tmp.append("Incorrect value for 'outageInfoAvailable'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageKey']).strip()=="" if response['outage']['outageKey'] is None else str(response['outage']['outageKey']).strip()):
                tmp = [str(document['outageKey']).strip(), str(response['outage']['outageKey']).strip()]
                tmp.append("Incorrect value for 'outageKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageTypeDescription']).strip()=="" if response['outage']['outageType'] is None else str(response['outage']['outageType']).strip()):
                tmp = [str(document['outageTypeDescription']).strip(), str(response['outage']['outageType']).strip()]
                tmp.append("Incorrect value for 'outageTypeDescription/outageType'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageTypeKey']).strip()=="" if response['outage']['outageTypekey'] is None else str(response['outage']['outageTypekey']).strip()):
                tmp = [str(document['abcd']).strip(), str(response['abcd']).strip()]
                tmp.append("Incorrect value for 'outageTypeKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outsourcer']).strip()=="" if response['outage']['outsourcer'] is None else str(response['outage']['outsourcer']).strip()):
                tmp = [str(document['outsourcer']).strip(), str(response['outage']['outsourcer']).strip()]
                tmp.append("Incorrect value for 'outsourcer'!")
                table.append(tmp)
                status = 1

            if not (str(document['overideOutage']).strip()=="" if response['outage']['overideOutage'] is None else str(response['outage']['overideOutage']).strip()):
                tmp = [str(document['overideOutage']).strip(), str(response['outage']['overideOutage']).strip()]
                tmp.append("Incorrect value for 'overideOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['platform']).strip()=="" if response['platform'] is None else str(response['platform']).strip()):
                tmp = [str(document['platform']).strip(), str(response['platform']).strip()]
                tmp.append("Incorrect value for 'platform'!")
                table.append(tmp)
                status = 1

            if not (str(document['previousOwnerSkill']).strip()=="" if response['outage']['previousOwnerSkill'] is None else str(response['outage']['previousOwnerSkill']).strip()):
                tmp = [str(document['previousOwnerSkill']).strip(), str(response['outage']['previousOwnerSkill']).strip()]
                tmp.append("Incorrect value for 'previousOwnerSkill'!")
                table.append(tmp)
                status = 1

            if not (str(document['previousTeam']).strip()=="" if response['outage']['previousTeam'] is None else str(response['outage']['previousTeam']).strip()):
                tmp = [str(document['previousTeam']).strip(), str(response['outage']['previousTeam']).strip()]
                tmp.append("Incorrect value for 'previousTeam'!")
                table.append(tmp)
                status = 1

            #if  keys>100:
            if not (str(document.get('priority', '')).strip()=="" if response['priority'] is None else str(response['priority']).strip()):
                tmp = [str(document['priority']).strip(), str(response['priority']).strip()]
                tmp.append("Incorrect value for 'priority'!")
                table.append(tmp)
                status = 1

            if not (str(document['priorityKey']).strip()=="" if response['priorityKey'] is None else str(response['priorityKey']).strip()):
                tmp = [str(document['priorityKey']).strip(), str(response['priorityKey']).strip()]
                tmp.append("Incorrect value for 'priorityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['processType']).strip()=="" if response['processType'] is None else str(response['processType']).strip()):
                tmp = [str(document['processType']).strip(), str(response['processType']).strip()]
                tmp.append("Incorrect value for 'processType'!")
                table.append(tmp)
                status = 1

            if not (str(document['processTypeDescription']).strip()=="" if response['processTypeDesc'] is None else str(response['processTypeDesc']).strip()):
                tmp = [str(document['processTypeDescription']).strip(), str(response['processTypeDesc']).strip()]
                tmp.append("Incorrect value for 'processTypeDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['productId']).strip()=="" if response['productId'] is None else str(response['productId']).strip()):
                tmp = [str(document['productId']).strip(), str(response['productId']).strip()]
                tmp.append("Incorrect value for 'productId'!")
                table.append(tmp)
                status = 1

            if not (str(document['productSeries']).strip()=="" if response['productSeries'] is None else str(response['productSeries']).strip()):
                tmp = [str(document['productSeries']).strip(), str(response['productSeries']).strip()]
                tmp.append("Incorrect value for 'productSeries'!")
                table.append(tmp)
                status = 1

            if not (str(document['raFa']).strip()=="" if response['outage']['raFa'] is None else str(response['outage']['raFa']).strip()):
                tmp = [str(document['raFa']).strip(), str(response['outage']['raFa']).strip()]
                tmp.append("Incorrect value for 'raFa'!")
                table.append(tmp)
                status = 1

            if not (str(document['reason']).strip()=="" if response['reason'] is None else str(response['reason']).strip()):
                tmp = [str(document['reason']).strip(), str(response['reason']).strip()]
                tmp.append("Incorrect value for 'reason'!")
                table.append(tmp)
                status = 1

            if not (str(document['release']).strip()=="" if response['release'] is None else str(response['release']).strip()):
                tmp = [str(document['release']).strip(), str(response['release']).strip()]
                tmp.append("Incorrect value for 'release'!")
                table.append(tmp)
                status = 1

            if not (str(document['reporterDetails']).strip()=="" if response['reporterDetails'] is None else str(response['reporterDetails']).strip()):
                tmp = [str(document['reporterDetails']).strip(), str(response['reporterDetails']).strip()]
                tmp.append("Incorrect value for 'reporterDetails'!")
                table.append(tmp)
                status = 1

            if not (str(document['routerName']).strip()=="" if response['outage']['routerName'] is None else str(response['outage']['routerName']).strip()):
                tmp = [str(document['routerName']).strip(), str(response['outage']['routerName']).strip()]
                tmp.append("Incorrect value for 'routerName'!")
                table.append(tmp)
                status = 1

            if not (str(document['secVulnerability']).strip()=="" if response['secVulnerability'] is None else str(response['secVulnerability']).strip()):
                tmp = [str(document['secVulnerability']).strip(), str(response['secVulnerability']).strip()]
                tmp.append("Incorrect value for 'secVulnerability'!")
                table.append(tmp)
                status = 1

            if not (str(document['serialNumber']).strip()=="" if response['serialNumber'] is None else str(response['serialNumber']).strip()):
                tmp = [str(document['serialNumber']).strip(), str(response['serialNumber']).strip()]
                tmp.append("Incorrect value for 'serialNumber'!")
                table.append(tmp)
                status = 1

            #if  keys>100:
            if not (str(document.get('severity', '')).strip()=="" if response['severity'] is None else str(response['severity']).strip()):
                tmp = [str(document['severity']).strip(), str(response['severity']).strip()]
                tmp.append("Incorrect value for 'severity'!")
                table.append(tmp)
                status = 1

            #if  keys>100:
            if not (str(document.get('severityKey', '')).strip()=="" if response['severityKey'] is None else str(response['severityKey']).strip()):
                tmp = [str(document['severityKey']).strip(), str(response['severityKey']).strip()]
                tmp.append("Incorrect value for 'severityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['sirtBundle']).strip()=="" if response['sirtBundle'] is None else str(response['sirtBundle']).strip()):
                tmp = [str(document['sirtBundle']).strip(), str(response['sirtBundle']).strip()]
                tmp.append("Incorrect value for 'sirtBundle'!")
                table.append(tmp)
                status = 1

            if not (str(document['sku']).strip()=="" if response['entitlement']['sku'] is None else str(response['entitlement']['sku']).strip()):
                tmp = [str(document['sku']).strip(), str(response['entitlement']['sku']).strip()]
                tmp.append("Incorrect value for 'sku'!")
                table.append(tmp)
                status = 1

            if not (str(document['smeContact']).strip()=="" if response['smeContact'] is None else str(response['smeContact']).strip()):
                tmp = [str(document['smeContact']).strip(), str(response['smeContact']).strip()]
                tmp.append("Incorrect value for 'smeContact'!")
                table.append(tmp)
                status = 1

            if not (str(document['software']).strip()=="" if response['software'] is None else str(response['software']).strip()):
                tmp = [str(document['software']).strip(), str(response['software']).strip()]
                tmp.append("Incorrect value for 'software'!")
                table.append(tmp)
                status = 1

            if not (str(document['specialRelease']).strip()=="" if response['specialRelease'] is None else str(response['specialRelease']).strip()):
                tmp = [str(document['specialRelease']).strip(), str(response['specialRelease']).strip()]
                tmp.append("Incorrect value for 'specialRelease'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory1']).strip()=="" if response['srCat1'] is None else str(response['srCat1']).strip()):
                tmp = [str(document['srCategory1']).strip(), str(response['srCat1']).strip()]
                tmp.append("Incorrect value for 'srCategory1'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory2']).strip()=="" if response['srCat2'] is None else str(response['srCat2']).strip()):
                tmp = [str(document['srCategory2']).strip(), str(response['srCat2']).strip()]
                tmp.append("Incorrect value for 'srCategory2'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory3']).strip()=="" if response['srCat3'] is None else str(response['srCat3']).strip()):
                tmp = [str(document['srCategory3']).strip(), str(response['srCat3']).strip()]
                tmp.append("Incorrect value for 'srCategory3'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory4']).strip()=="" if response['srCat4'] is None else str(response['srCat4']).strip()):
                tmp = [str(document['srCategory4']).strip(), str(response['srCat4']).strip()]
                tmp.append("Incorrect value for 'srCategory4'!")
                table.append(tmp)
                status = 1

            if not (str(document['startDate']).strip()=="" if response['entitlement']['startDate'] is None else str(response['entitlement']['startDate']).strip()):
                tmp = [str(document['startDate']).strip(), str(response['entitlement']['startDate']).strip()]
                tmp.append("Incorrect value for 'startDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['status']).strip()=="" if response['status'] is None else str(response['status']).strip()):
                tmp = [str(document['status']).strip(), str(response['status']).strip()]
                tmp.append("Incorrect value for 'status'!")
                table.append(tmp)
                status = 1

            if not (str(document['statusKey']).strip()=="" if response['statusKey'] is None else str(response['statusKey']).strip()):
                tmp = [str(document['statusKey']).strip(), str(response['statusKey']).strip()]
                tmp.append("Incorrect value for 'statusKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory1']).strip()=="" if response['techCat1'] is None else str(response['techCat1']).strip()):
                tmp = [str(document['technicalCategory1']).strip(), str(response['techCat1']).strip()]
                tmp.append("Incorrect value for 'technicalCategory1'!")
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory2']).strip()=="" if response['techCat2'] is None else str(response['techCat2']).strip()):
                tmp = [str(document['technicalCategory2']).strip(), str(response['techCat2']).strip()]
                tmp.append("Incorrect value for 'technicalCategory2'!")
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory3']).strip()=="" if response['techCat3'] is None else str(response['techCat3']).strip()):
                tmp = [str(document['technicalCategory3']).strip(), str(response['techCat3']).strip()]
                tmp.append("Incorrect value for 'technicalCategory3'!")
                table.append(tmp)
                status = 1

            if not (str(document['temperature']).strip()=="" if response['outage']['temperature'] is None else str(response['outage']['temperature']).strip()):
                tmp = [str(document['temperature']).strip(), str(response['outage']['temperature']).strip()]
                tmp.append("Incorrect value for 'temperature'!")
                table.append(tmp)
                status = 1

            if not (str(document['theaterDescription']).strip()=="" if response['outage']['theater'] is None else str(response['outage']['theater']).strip()):
                tmp = [str(document['theaterDescription']).strip(), str(response['outage']['theater']).strip()]
                tmp.append("Incorrect value for 'theaterDescription/theater'!")
                table.append(tmp)
                status = 1

            if not (str(document['theaterKey']).strip()=="" if response['outage']['theaterkey'] is None else str(response['outage']['theaterkey']).strip()):
                tmp = [str(document['theaterKey']).strip(), str(response['outage']['theaterkey']).strip()]
                tmp.append("Incorrect value for 'theaterKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['top5']).strip()=="" if response['outage']['top5'] is None else str(response['outage']['top5']).strip()):
                tmp = [str(document['top5']).strip(), str(response['outage']['top5']).strip()]
                tmp.append("Incorrect value for 'top5'!")
                table.append(tmp)
                status = 1

            if not (str(document['totalOutageTime']).strip()=="" if response['outage']['totalOutageTime'] is None else str(response['outage']['totalOutageTime']).strip()):
                tmp = [str(document['totalOutageTime']).strip(), str(response['outage']['totalOutageTime']).strip()]
                tmp.append("Incorrect value for 'totalOutageTime'!")
                table.append(tmp)
                status = 1

            #if  keys>100:
            if not (str(document.get('urgency', '')).strip()=="" if response['urgency'] is None else str(response['urgency']).strip()):
                tmp = [str(document['urgency']).strip(), str(response['urgency']).strip()]
                tmp.append("Incorrect value for 'urgency'!")
                table.append(tmp)
                status = 1

            if not (str(document['urgencyKey']).strip()=="" if response['urgencyKey'] is None else str(response['urgencyKey']).strip()):
                tmp = [str(document['urgencyKey']).strip(), str(response['urgencyKey']).strip()]
                tmp.append("Incorrect value for 'urgencyKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['version']).strip()=="" if response['version'] is None else str(response['version']).strip()):
                tmp = [str(document['version']).strip(), str(response['version']).strip()]
                tmp.append("Incorrect value for 'version'!")
                table.append(tmp)
                status = 1

            if not (str(document['viaDescription']).strip()=="" if response['outage']['via'] is None else str(response['outage']['via']).strip()):
                tmp = [str(document['viaDescription']).strip(), str(response['via']).strip()]
                tmp.append("Incorrect value for 'viaDescription/via'!")
                table.append(tmp)
                status = 1

            if not (str(document['viaKey']).strip()=="" if response['outage']['viaKey'] is None else str(response['outage']['viaKey']).strip()):
                tmp = [str(document['viaKey']).strip(), str(response['viaKey']).strip()]
                tmp.append("Incorrect value for 'viaKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['warrantyEndDate']).strip()=="" if response['entitlement']['warrantyEndDate'] is None else str(response['entitlement']['warrantyEndDate']).strip()):
                tmp = [str(document['warrantyEndDate']).strip(), str(response['entitlement']['warrantyEndDate']).strip()]
                tmp.append("Incorrect value for 'warrantyEndDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['warrantyEndDate']).strip()=="" if response['entitlement']['warrantyEndDate'] is None else str(response['entitlement']['warrantyEndDate']).strip()):
                tmp = [str(document['warrantyEndDate']).strip(), str(response['entitlement']['warrantyEndDate']).strip()]
                tmp.append("Incorrect value for 'warrantyEndDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['yearRoundSupport']).strip()=="" if response['outage']['support24X7'] is None else str(response['outage']['support24X7']).strip()):
                tmp = [str(document['yearRoundSupport']).strip(), str(response['outage']['support24X7']).strip()]
                tmp.append("Incorrect value for 'yearRoundSupport/support24X7'!")
                table.append(tmp)
                status = 1

            #print json.dumps(response, indent=4, sort_keys=True)
            if not (str(document['zzQ1']).strip()=="" if response['outage']['zzq1'] is None else str(response['outage']['zzq1']).strip()):
                tmp = [str(document['zzQ1']).strip(), str(response['outage']['zzq1']).strip()]
                tmp.append("Incorrect value for 'zzQ1'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ2']).strip()=="" if response['outage']['zzq2'] is None else str(response['outage']['zzq2']).strip()):
                tmp = [str(document['zzQ2']).strip(), str(response['outage']['zzq2']).strip()]
                tmp.append("Incorrect value for 'zzQ2'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ3']).strip()=="" if response['outage']['zzq3'] is None else str(response['outage']['zzq3']).strip()):
                tmp = [str(document['zzQ3']).strip(), str(response['outage']['zzq3']).strip()]
                tmp.append("Incorrect value for 'zzQ3'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ4']).strip()=="" if response['outage']['zzq4'] is None else str(response['outage']['zzq4']).strip()):
                tmp = [str(document['zzQ4']).strip(), str(response['outage']['zzq4']).strip()]
                tmp.append("Incorrect value for 'zzQ4'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ5']).strip()=="" if response['outage']['zzq5'] is None else str(response['outage']['zzq5']).strip()):
                tmp = [str(document['zzQ5']).strip(), str(response['outage']['zzq5']).strip()]
                tmp.append("Incorrect value for 'zzQ5'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ6']).strip()=="" if response['outage']['zzq6'] is None else str(response['outage']['zzq6']).strip()):
                tmp = [str(document['zzQ6']).strip(), str(response['outage']['zzq6']).strip()]
                tmp.append("Incorrect value for 'zzQ6'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ7']).strip()=="" if response['outage']['zzq7'] is None else str(response['outage']['zzq7']).strip()):
                tmp = [str(document['zzQ7']).strip(), str(response['outage']['zzq7']).strip()]
                tmp.append("Incorrect value for 'zzQ7'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ8']).strip()=="" if response['outage']['zzq8'] is None else str(response['outage']['zzq8']).strip()):
                tmp = [str(document['zzQ8']).strip(), str(response['outage']['zzq8']).strip()]
                tmp.append("Incorrect value for 'zzQ8'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ9']).strip()=="" if response['outage']['zzq9'] is None else str(response['outage']['zzq9']).strip()):
                tmp = [str(document['zzQ9']).strip(), str(response['outage']['zzq9']).strip()]
                tmp.append("Incorrect value for 'zzQ9'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ10']).strip()=="" if response['outage']['zzq10'] is None else str(response['outage']['zzq10']).strip()):
                tmp = [str(document['zzQ10']).strip(), str(response['outage']['zzq10']).strip()]
                tmp.append("Incorrect value for 'zzQ10'!")
                table.append(tmp)
                status = 1
                row.append("No Match Found")

            if status==0:
                print "Match Found"
                row.append("Match Found")
            else:
                print tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
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


#client = MongoClient('10.219.48.134', 27017)
client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['srDetails']
api = HBase()
document_no = 0
documents = collection.find({})
#documents = collection.find({'caseId':'2015-1009-T-0652'})
ofile = open('HBaseAPIOutput.csv', "wb")
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
        print document['endDate']
    except Exception:
        print Exception.message
        print(traceback.format_exc())
    writer.writerow(row)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "\n\n"
ofile.close()

