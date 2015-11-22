__author__ = 'asifj'
import requests
from pymongo import MongoClient
from bson import Binary, Code
import json
import csv
import traceback
import logging
from tabulate import tabulate
import datetime

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
        keys = len(document.keys())
        print "Keys: "+str(keys)
        row.append(r.status_code)
        status = 0
        if r.status_code==200:
            response = json.loads(r.text)
            #print json.dumps(response, indent=4)
            table = []
            if not (str(document['betaType']).strip()) == ("" if response['betaType'] is None else str(response['betaType']).strip()):
                tmp = [str(document['betaType']).strip(), str(response['betaType']).strip()]
                tmp.append("Incorrect value for 'betaType'!")
                table.append(tmp)
                status = 1

            if not (str(document['build']).strip()) == ("" if response['outage']['build'] is None else str(response['outage']['build']).strip()):
                tmp = [str(document['build']).strip(), str(response['outage']['build']).strip()]
                tmp.append("Incorrect value for 'build'!")
                table.append(tmp)
                status = 1

            if not (str(document['ccEngineer']).strip()) == ("" if response['outage']['ccEngineer'] is None else str(response['outage']['ccEngineer']).strip()):
                tmp = [str(document['ccEngineer']).strip(), str(response['outage']['ccEngineer']).strip()]
                tmp.append("Incorrect value for 'ccEngineer'!")
                table.append(tmp)
                status = 1

            if not (str(document['caseId']).strip()) == ("" if response['srId'] is None else str(response['srId']).strip()):
                tmp = [str(document['caseId']).strip(), str(response['srId']).strip()]
                tmp.append("Incorrect value for 'caseId'!")
                table.append(tmp)
                status = 1

            if not (str(document['contractId']).strip()) == ("" if response['entitlement']['contractId'] is None else str(response['entitlement']['contractId']).strip()):
                tmp = [str(document['contractId']).strip(), str(response['entitlement']['contractId']).strip()]
                tmp.append("Incorrect value for 'contractId'!")
                table.append(tmp)
                status = 1

            if not (str(document['contractStatus']).strip()) == ("" if response['entitlement']['contractStatus'] is None else str(response['entitlement']['contractStatus']).strip()):
                tmp = [str(document['contractStatus']).strip(), str(response['entitlement']['contractStatus']).strip()]
                tmp.append("Incorrect value for 'contractStatus'!")
                table.append(tmp)
                status = 1

            if not (str(document['country']).strip()) == ("" if response['outage']['country'] is None else str(response['outage']['country']).strip()):
                tmp = [str(document['country']).strip(), str(response['outage']['country']).strip()]
                tmp.append("Incorrect value for 'country'!")
                table.append(tmp)
                status = 1

            if not (str(document['courtesyDescription']).strip()) == ("" if response['courtesy'] is None else str(response['courtesy']).strip()):
                tmp = [str(document['courtesyDescription']).strip(), str(response['courtesy']).strip()]
                tmp.append("Incorrect value for 'courtesyDescription/courtesy'!")
                table.append(tmp)
                status = 1

            if not (str(document['courtesykey']).strip()) == ("" if response['courtesyKey'] is None else str(response['courtesyKey']).strip()):
                tmp = [str(document['courtesykey']).strip(), str(response['courtesyKey']).strip()]
                tmp.append("Incorrect value for 'courtesykey'!")
                table.append(tmp)
                status = 1

            if not (str(document['criticalIssue']).strip()) == ("" if response['outage']['criticalIssue'] is None else str(response['outage']['criticalIssue']).strip()):
                tmp = [str(document['criticalIssue']).strip(), str(response['outage']['criticalIssue']).strip()]
                tmp.append("Incorrect value for 'criticalIssue'!")
                table.append(tmp)
                status = 1

            if not (str(document['criticalOutage']).strip()) == ("" if response['criticalOutage'] is None else str(response['criticalOutage']).strip()):
                tmp = [str(document['criticalOutage']).strip(), str(response['criticalOutage']).strip()]
                tmp.append("Incorrect value for 'criticalOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['customerCaseNumber']).strip()) == ("" if response['outage']['custCaseNo'] is None else str(response['outage']['custCaseNo']).strip()):
                tmp = [str(document['customerCaseNumber']).strip(), str(response['outage']['custCaseNo']).strip()]
                tmp.append("Incorrect value for 'customerCaseNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['cve']).strip()) == ("" if response['cve'] is None else str(response['cve']).strip()):
                tmp = [str(document['cve']).strip(), str(response['cve']).strip()]
                tmp.append("Incorrect value for 'cve'!")
                table.append(tmp)
                status = 1

            if not (str(document['cvss']).strip()) == ("" if response['cvss'] is None else str(response['cvss']).strip()):
                tmp = [str(document['cvss']).strip(), str(response['cvss']).strip()]
                tmp.append("Incorrect value for 'cvss'!")
                table.append(tmp)
                status = 1

            if not (str(document['description']).strip()) == ("" if response['desc'] is None else str(response['desc']).strip()):
                #tmp = [str(document['description']).strip(), str(response['desc']).strip(), "Incorrect value for 'description'!"]
                tmp = [str(document['description']).strip()[:10], str(response['desc']).strip()[:10], "Incorrect value for 'description'!"]
                table.append(tmp)
                status = 1

            endDate = ""
            try:
                if response['entitlement']['endDate'] is None:
                    endDate = "00000000"
                else:
                    endDate = datetime.datetime.fromtimestamp(float(endDate)/1000).strftime('%Y%m%d')
            except Exception:
                print "EndDate issue: "+str(Exception.message)
                print(traceback.format_exc())
                endDate = endDate.replace("00:00:00", "")
                endDate = endDate.replace("-", "")
                print "endDate: "+str(endDate)

            if not (str(document['endDate']).strip()) == endDate.strip():
                tmp = [str(document['endDate']).strip(), endDate, "Incorrect value for 'endDate'!"]
                table.append(tmp)
                status = 1

            if not (str(document['entitledSerialNumber']).strip()) == ("" if response['entitlement']['entitledSerialNumber'] is None else str(response['entitlement']['entitledSerialNumber']).strip()):
                tmp = [str(document['entitledSerialNumber']).strip(), str(response['entitlement']['entitledSerialNumber']).strip()]
                tmp.append("Incorrect value for 'entitledSerialNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementChecked']).strip()) == ("" if response['entitlement']['entitlementChecked'] is None else str(response['entitlement']['entitlementChecked']).strip()):
                tmp = [str(document['entitlementChecked']).strip(), str(response['entitlement']['entitlementChecked']).strip()]
                tmp.append("Incorrect value for 'entitlementChecked'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementServiceLevel']).strip()) == ("" if response['entitlement']['entitlementServiceLevel'] is None else str(response['entitlement']['entitlementServiceLevel']).strip()):
                tmp = [str(document['entitlementServiceLevel']).strip(), str(response['entitlement']['entitlementServiceLevel']).strip()]
                tmp.append("Incorrect value for 'entitlementServiceLevel'!")
                table.append(tmp)
                status = 1

            if not (str(document['entitlementSource']).strip()) == ("" if response['entitlement']['entitlementSource'] is None else str(response['entitlement']['entitlementSource']).strip()):
                tmp = [str(document['entitlementSource']).strip(), str(response['entitlement']['entitlementSource']).strip()]
                tmp.append("Incorrect value for 'entitlementSource'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('escalation', '')).strip()) == ("" if response['escalationDesc'] is None else str(response['escalationDesc']).strip()):
                tmp = [str(document['escalation']).strip(), str(response['escalationDesc']).strip()]
                tmp.append("Incorrect value for 'escalation'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationLevelDescription']).strip()) == ("" if response['outage']['escalationLevel'] is None else str(response['outage']['escalationLevel']).strip()):
                tmp = [str(document['escalationLevelDescription']).strip(), str(response['outage']['escalationLevel']).strip()]
                tmp.append("Incorrect value for 'escalationLevelDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationLevelKey']).strip()) == ("" if response['outage']['escalationLevelkey'] is None else str(response['outage']['escalationLevelkey']).strip()):
                tmp = [str(document['escalationLevelKey']).strip(), str(response['outage']['escalationLevelkey']).strip()]
                tmp.append("Incorrect value for 'escalationLevelKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['escalationkey']).strip()) == ("" if response['escalationKey'] is None else str(response['escalationKey']).strip()):
                tmp = [str(document['escalationkey']).strip(), str(response['escalationKey']).strip()]
                tmp.append("Incorrect value for 'escalationkey'!")
                table.append(tmp)
                status = 1

            if not (str(document['externallyReported']).strip()) == ("" if response['externallyReported'] is None else str(response['externallyReported']).strip()):
                tmp = [str(document['externallyReported']).strip(), str(response['externallyReported']).strip()]
                tmp.append("Incorrect value for 'externallyReported'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('followupMethod', '')).strip()) == ("" if response['outage']['followUpMethod'] is None else str(response['outage']['followUpMethod']).strip()):
                tmp = [str(document['followupMethod']).strip(), str(response['outage']['followUpMethod']).strip()]
                tmp.append("Incorrect value for 'followupMethod'!")
                table.append(tmp)
                status = 1

            if not (str(document['followupMethodKey']).strip()) == ("" if response['outage']['followUpMethodkey'] is None else str(response['outage']['followUpMethodkey']).strip()):
                tmp = [str(document['followupMethodKey']).strip(), str(response['outage']['followUpMethodkey']).strip()]
                tmp.append("Incorrect value for 'followupMethodKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['jsaAdvisoryBoard']).strip()) == ("" if response['jsaAdvisoryBoard'] is None else str(response['jsaAdvisoryBoard']).strip()):
                tmp = [str(document['jsaAdvisoryBoard']).strip(), str(response['jsaAdvisoryBoard']).strip()]
                tmp.append("Incorrect value for 'jsaAdvisoryBoard'!")
                table.append(tmp)
                status = 1

            if not (str(document['jtac']).strip()) == ("" if response['jtac'] is None else str(response['jtac']).strip()):
                tmp = [str(document['jtac']).strip(), str(response['jtac']).strip()]
                tmp.append("Incorrect value for 'jtac'!")
                table.append(tmp)
                status = 1

            if not (str(document['knowledgeArticle']).strip()) == ("" if response['outage']['knowledgeArticle'] is None else str(response['outage']['knowledgeArticle']).strip()):
                tmp = [str(document['knowledgeArticle']).strip(), str(response['outage']['knowledgeArticle']).strip()]
                tmp.append("Incorrect value for 'knowledgeArticle'!")
                table.append(tmp)
                status = 1

            ocd = document.get('outageCauseDescription', "")
            if ocd=="":
                ocd = document.get('ouatgeCauseDescription', "")
            if not (str(ocd).strip()) == ("" if response['outage']['outageCause'] is None else str(response['outage']['outageCause']).strip()):
                tmp = [str(ocd).strip(), str(response['outage']['outageCause']).strip()]
                tmp.append("Incorrect value for 'outageCauseDescription/outageCause'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageCauseKey']).strip()) == ("" if response['outage']['outageCausekey'] is None else str(response['outage']['outageCausekey']).strip()):
                tmp = [str(document['outageCauseKey']).strip(), str(response['outage']['outageCausekey']).strip()]
                tmp.append("Incorrect value for 'outageCauseKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageDescription']).strip()) == ("" if response['outage']['outage'] is None else str(response['outage']['outage']).strip()):
                tmp = [str(document['outageDescription']).strip(), str(response['outage']['outage']).strip()]
                tmp.append("Incorrect value for 'outageDescription/outage'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageImpactKey']).strip()) == ("" if response['outage']['outageImpactKey'] is None else str(response['outage']['outageImpactKey']).strip()):
                tmp = [str(document['outageImpactKey']).strip(), str(response['outage']['outageImpactKey']).strip()]
                tmp.append("Incorrect value for 'outageImpactKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageInfoAvailable']).strip()) == ("" if response['outage']['outageInfoAvailable'] is None else str(response['outage']['outageInfoAvailable']).strip()):
                tmp = [str(document['outageInfoAvailable']).strip(), str(response['outage']['outageInfoAvailable']).strip()]
                tmp.append("Incorrect value for 'outageInfoAvailable'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageKey']).strip()) == ("" if response['outage']['outageKey'] is None else str(response['outage']['outageKey']).strip()):
                tmp = [str(document['outageKey']).strip(), str(response['outage']['outageKey']).strip()]
                tmp.append("Incorrect value for 'outageKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageTypeDescription']).strip()) == ("" if response['outage']['outageType'] is None else str(response['outage']['outageType']).strip()):
                tmp = [str(document['outageTypeDescription']).strip(), str(response['outage']['outageType']).strip()]
                tmp.append("Incorrect value for 'outageTypeDescription/outageType'!")
                table.append(tmp)
                status = 1

            if not (str(document['outageTypeKey']).strip()) == ("" if response['outage']['outageTypekey'] is None else str(response['outage']['outageTypekey']).strip()):
                tmp = [str(document['outageTypeKey']).strip(), str(response['outage']['outageTypekey']).strip()]
                tmp.append("Incorrect value for 'outageTypeKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['outsourcer']).strip()) == ("" if response['outage']['outsourcer'] is None else str(response['outage']['outsourcer']).strip()):
                tmp = [str(document['outsourcer']).strip(), str(response['outage']['outsourcer']).strip()]
                tmp.append("Incorrect value for 'outsourcer'!")
                table.append(tmp)
                status = 1

            if not (str(document['overideOutage']).strip()) == ("" if response['outage']['overideOutage'] is None else str(response['outage']['overideOutage']).strip()):
                tmp = [str(document['overideOutage']).strip(), str(response['outage']['overideOutage']).strip()]
                tmp.append("Incorrect value for 'overideOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['platform']).strip()) == ("" if response['platform'] is None else str(response['platform']).strip()):
                tmp = [str(document['platform']).strip(), str(response['platform']).strip()]
                tmp.append("Incorrect value for 'platform'!")
                table.append(tmp)
                status = 1

            if not (str(document['previousOwnerSkill']).strip()) == ("" if response['outage']['previousOwnerSkill'] is None else str(response['outage']['previousOwnerSkill']).strip()):
                tmp = [str(document['previousOwnerSkill']).strip(), str(response['outage']['previousOwnerSkill']).strip()]
                tmp.append("Incorrect value for 'previousOwnerSkill'!")
                table.append(tmp)
                status = 1

            if not (str(document['previousTeam']).strip()) == ("" if response['outage']['previousTeam'] is None else str(response['outage']['previousTeam']).strip()):
                tmp = [str(document['previousTeam']).strip(), str(response['outage']['previousTeam']).strip()]
                tmp.append("Incorrect value for 'previousTeam'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('priority', '')).strip()) == ("" if response['priority'] is None else str(response['priority']).strip()):
                tmp = [str(document['priority']).strip(), str(response['priority']).strip()]
                tmp.append("Incorrect value for 'priority'!")
                table.append(tmp)
                status = 1

            if not (str(document['priorityKey']).strip()) == ("" if response['priorityKey'] is None else str(response['priorityKey']).strip()):
                tmp = [str(document['priorityKey']).strip(), str(response['priorityKey']).strip()]
                tmp.append("Incorrect value for 'priorityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['processType']).strip()) == ("" if response['processType'] is None else str(response['processType']).strip()):
                tmp = [str(document['processType']).strip(), str(response['processType']).strip()]
                tmp.append("Incorrect value for 'processType'!")
                table.append(tmp)
                status = 1

            if not (str(document['processTypeDescription']).strip()) == ("" if response['processTypeDesc'] is None else str(response['processTypeDesc']).strip()):
                tmp = [str(document['processTypeDescription']).strip(), str(response['processTypeDesc']).strip()]
                tmp.append("Incorrect value for 'processTypeDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['productId']).strip()) == ("" if response['productId'] is None else str(response['productId']).strip()):
                tmp = [str(document['productId']).strip(), str(response['productId']).strip()]
                tmp.append("Incorrect value for 'productId'!")
                table.append(tmp)
                status = 1

            if not (str(document['productSeries']).strip()) == ("" if response['productSeries'] is None else str(response['productSeries']).strip()):
                tmp = [str(document['productSeries']).strip(), str(response['productSeries']).strip()]
                tmp.append("Incorrect value for 'productSeries'!")
                table.append(tmp)
                status = 1

            if not (str(document['raFa']).strip()) == ("" if response['outage']['raFa'] is None else str(response['outage']['raFa']).strip()):
                tmp = [str(document['raFa']).strip(), str(response['outage']['raFa']).strip()]
                tmp.append("Incorrect value for 'raFa'!")
                table.append(tmp)
                status = 1

            if not (str(document['reason']).strip()) == ("" if response['reason'] is None else str(response['reason']).strip()):
                tmp = [str(document['reason']).strip(), str(response['reason']).strip()]
                tmp.append("Incorrect value for 'reason'!")
                table.append(tmp)
                status = 1

            if not (str(document['release']).strip()) == ("" if response['release'] is None else str(response['release']).strip()):
                tmp = [str(document['release']).strip(), str(response['release']).strip()]
                tmp.append("Incorrect value for 'release'!")
                table.append(tmp)
                status = 1

            if not (str(document['reporterDetails']).strip()) == ("" if response['reporterDetails'] is None else str(response['reporterDetails']).strip()):
                tmp = [str(document['reporterDetails']).strip(), str(response['reporterDetails']).strip()]
                tmp.append("Incorrect value for 'reporterDetails'!")
                table.append(tmp)
                status = 1

            if not (str(document['routerName']).strip()) == ("" if response['outage']['routerName'] is None else str(response['outage']['routerName']).strip()):
                tmp = [str(document['routerName']).strip(), str(response['outage']['routerName']).strip()]
                tmp.append("Incorrect value for 'routerName'!")
                table.append(tmp)
                status = 1

            if not (str(document['secVulnerability']).strip()) == ("" if response['secVulnerability'] is None else str(response['secVulnerability']).strip()):
                tmp = [str(document['secVulnerability']).strip(), str(response['secVulnerability']).strip()]
                tmp.append("Incorrect value for 'secVulnerability'!")
                table.append(tmp)
                status = 1

            if not (str(document['serialNumber']).strip()) == ("" if response['serialNumber'] is None else str(response['serialNumber']).strip()):
                tmp = [str(document['serialNumber']).strip(), str(response['serialNumber']).strip()]
                tmp.append("Incorrect value for 'serialNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('severity', '')).strip()) == ("" if response['severity'] is None else str(response['severity']).strip()):
                tmp = [str(document['severity']).strip(), str(response['severity']).strip()]
                tmp.append("Incorrect value for 'severity'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('severityKey', '')).strip()) == ("" if response['severityKey'] is None else str(response['severityKey']).strip()):
                tmp = [str(document['severityKey']).strip(), str(response['severityKey']).strip()]
                tmp.append("Incorrect value for 'severityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['sirtBundle']).strip()) == ("" if response['sirtBundle'] is None else str(response['sirtBundle']).strip()):
                tmp = [str(document['sirtBundle']).strip(), str(response['sirtBundle']).strip()]
                tmp.append("Incorrect value for 'sirtBundle'!")
                table.append(tmp)
                status = 1

            if not (str(document['sku']).strip()) == ("" if response['entitlement']['sku'] is None else str(response['entitlement']['sku']).strip()):
                tmp = [str(document['sku']).strip(), str(response['entitlement']['sku']).strip()]
                tmp.append("Incorrect value for 'sku'!")
                table.append(tmp)
                status = 1

            if not (str(document['smeContact']).strip()) == ("" if response['smeContact'] is None else str(response['smeContact']).strip()):
                tmp = [str(document['smeContact']).strip(), str(response['smeContact']).strip()]
                tmp.append("Incorrect value for 'smeContact'!")
                table.append(tmp)
                status = 1

            if not (str(document['software']).strip()) == ("" if response['software'] is None else str(response['software']).strip()):
                tmp = [str(document['software']).strip(), str(response['software']).strip()]
                tmp.append("Incorrect value for 'software'!")
                table.append(tmp)
                status = 1

            if not (str(document['specialRelease']).strip()) == ("" if response['specialRelease'] is None else str(response['specialRelease']).strip()):
                tmp = [str(document['specialRelease']).strip(), str(response['specialRelease']).strip()]
                tmp.append("Incorrect value for 'specialRelease'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory1']).strip()) == ("" if response['srCat1'] is None else str(response['srCat1']).strip()):
                tmp = [str(document['srCategory1']).strip(), str(response['srCat1']).strip()]
                tmp.append("Incorrect value for 'srCategory1'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory2']).strip()) == ("" if response['srCat2'] is None else str(response['srCat2']).strip()):
                tmp = [str(document['srCategory2']).strip(), str(response['srCat2']).strip()]
                tmp.append("Incorrect value for 'srCategory2'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory3']).strip()) == ("" if response['srCat3'] is None else str(response['srCat3']).strip()):
                tmp = [str(document['srCategory3']).strip(), str(response['srCat3']).strip()]
                tmp.append("Incorrect value for 'srCategory3'!")
                table.append(tmp)
                status = 1

            if not (str(document['srCategory4']).strip()) == ("" if response['srCat4'] is None else str(response['srCat4']).strip()):
                tmp = [str(document['srCategory4']).strip(), str(response['srCat4']).strip()]
                tmp.append("Incorrect value for 'srCategory4'!")
                table.append(tmp)
                status = 1

            startDate = ""
            try:
                if response['entitlement']['startDate'] is None:
                    startDate = "00000000"
                else:
                    startDate = datetime.datetime.fromtimestamp(float(startDate)/1000).strftime('%Y%m%d')
            except Exception:
                print "StartDate issue: "+str(Exception.message)
                print(traceback.format_exc())
                startDate = startDate.replace("00:00:00", "")
                startDate = startDate.replace("-", "")
                print "StartDate: "+str(startDate)

            if not (str(document['startDate']).strip()) == startDate.strip():
                tmp = [str(document['startDate']).strip(), startDate, "Incorrect value for 'startDate'!"]
                table.append(tmp)
                status = 1

            if not (str(document['status']).strip()) == ("" if response['status'] is None else str(response['status']).strip()):
                tmp = [str(document['status']).strip(), str(response['status']).strip(),
                       "Incorrect value for 'status'!"]
                table.append(tmp)
                status = 1

            if not (str(document['statusKey']).strip()) == ("" if response['statusKey'] is None else str(response['statusKey']).strip()):
                tmp = [str(document['statusKey']).strip(), str(response['statusKey']).strip(),
                       "Incorrect value for 'statusKey'!"]
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory2']).strip()) == ("" if response['techCat1'] is None else str(response['techCat1']).strip()):
                tmp = [str(document['technicalCategory2']).strip(), str(response['techCat1']).strip(),
                       "Incorrect value for 'technicalCategory2'!"]
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory3']).strip()) == ("" if response['techCat2'] is None else str(response['techCat2']).strip()):
                tmp = [str(document['technicalCategory3']).strip(), str(response['techCat2']).strip(),
                       "Incorrect value for 'technicalCategory3'!"]
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory4']).strip()) == ("" if response['techCat3'] is None else str(response['techCat3']).strip()):
                tmp = [str(document['technicalCategory4']).strip(), str(response['techCat3']).strip(),
                       "Incorrect value for 'technicalCategory4'!"]
                table.append(tmp)
                status = 1

            if not (str(document['temperature']).strip()) == ("" if response['outage']['temperature'] is None else str(response['outage']['temperature']).strip()):
                tmp = [str(document['temperature']).strip(), str(response['outage']['temperature']).strip()]
                tmp.append("Incorrect value for 'temperature'!")
                table.append(tmp)
                status = 1

            if not (str(document['theaterDescription']).strip()) == ("" if response['outage']['theater'] is None else str(response['outage']['theater']).strip()):
                tmp = [str(document['theaterDescription']).strip(), str(response['outage']['theater']).strip()]
                tmp.append("Incorrect value for 'theaterDescription/theater'!")
                table.append(tmp)
                status = 1

            if not (str(document['theaterKey']).strip()) == ("" if response['outage']['theaterkey'] is None else str(response['outage']['theaterkey']).strip()):
                tmp = [str(document['theaterKey']).strip(), str(response['outage']['theaterkey']).strip()]
                tmp.append("Incorrect value for 'theaterKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['top5']).strip()) == ("" if response['outage']['top5'] is None else str(response['outage']['top5']).strip()):
                tmp = [str(document['top5']).strip(), str(response['outage']['top5']).strip()]
                tmp.append("Incorrect value for 'top5'!")
                table.append(tmp)
                status = 1

            if not (str(document['totalOutageTime']).strip()) == ("" if response['outage']['totalOutageTime'] is None else str(response['outage']['totalOutageTime']).strip()):
                tmp = [str(document['totalOutageTime']).strip(), str(response['outage']['totalOutageTime']).strip()]
                tmp.append("Incorrect value for 'totalOutageTime'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('urgency', '')).strip()) == ("" if response['urgency'] is None else str(response['urgency']).strip()):
                tmp = [str(document['urgency']).strip(), str(response['urgency']).strip()]
                tmp.append("Incorrect value for 'urgency'!")
                table.append(tmp)
                status = 1

            if not (str(document['urgencyKey']).strip()) == ("" if response['urgencyKey'] is None else str(response['urgencyKey']).strip()):
                tmp = [str(document['urgencyKey']).strip(), str(response['urgencyKey']).strip()]
                tmp.append("Incorrect value for 'urgencyKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['version']).strip()) == ("" if response['version'] is None else str(response['version']).strip()):
                tmp = [str(document['version']).strip(), str(response['version']).strip()]
                tmp.append("Incorrect value for 'version'!")
                table.append(tmp)
                status = 1

            if not (str(document['viaDescription']).strip()) == ("" if response['outage']['via'] is None else str(response['outage']['via']).strip()):
                tmp = [str(document['viaDescription']).strip(), str(response['outage']['via']).strip()]
                tmp.append("Incorrect value for 'viaDescription/via'!")
                table.append(tmp)
                status = 1

            if not (str(document['viaKey']).strip()) == ("" if response['outage']['viaKey'] is None else str(response['outage']['viaKey']).strip()):
                tmp = [str(document['viaKey']).strip(), str(response['outage']['viaKey']).strip()]
                tmp.append("Incorrect value for 'viaKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['warrantyEndDate']).strip()) == ("00000000" if response['entitlement']['warrantyEndDate'] is None else str(response['entitlement']['warrantyEndDate']).strip()):
                tmp = [str(document['warrantyEndDate']).strip(), str(response['entitlement']['warrantyEndDate']).strip()]
                tmp.append("Incorrect value for 'warrantyEndDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['yearRoundSupport']).strip()) == ("" if response['outage']['support24X7'] is None else str(response['outage']['support24X7']).strip()):
                tmp = [str(document['yearRoundSupport']).strip(), str(response['outage']['support24X7']).strip()]
                tmp.append("Incorrect value for 'yearRoundSupport/support24X7'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ1']).strip()) == ("" if response['outage']['zzq1'] is None else str(response['outage']['zzq1']).strip()):
                tmp = [str(document['zzQ1']).strip(), str(response['outage']['zzq1']).strip()]
                tmp.append("Incorrect value for 'zzQ1'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ2']).strip()) == ("" if response['outage']['zzq2'] is None else str(response['outage']['zzq2']).strip()):
                tmp = [str(document['zzQ2']).strip(), str(response['outage']['zzq2']).strip()]
                tmp.append("Incorrect value for 'zzQ2'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ3']).strip()) == ("" if response['outage']['zzq3'] is None else str(response['outage']['zzq3']).strip()):
                tmp = [str(document['zzQ3']).strip(), str(response['outage']['zzq3']).strip()]
                tmp.append("Incorrect value for 'zzQ3'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ4']).strip()) == ("" if response['outage']['zzq4'] is None else str(response['outage']['zzq4']).strip()):
                tmp = [str(document['zzQ4']).strip(), str(response['outage']['zzq4']).strip()]
                tmp.append("Incorrect value for 'zzQ4'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ5']).strip()) == ("" if response['outage']['zzq5'] is None else str(response['outage']['zzq5']).strip()):
                tmp = [str(document['zzQ5']).strip(), str(response['outage']['zzq5']).strip()]
                tmp.append("Incorrect value for 'zzQ5'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ6']).strip()) == ("" if response['outage']['zzq6'] is None else str(response['outage']['zzq6']).strip()):
                tmp = [str(document['zzQ6']).strip(), str(response['outage']['zzq6']).strip()]
                tmp.append("Incorrect value for 'zzQ6'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ7']).strip()) == ("" if response['outage']['zzq7'] is None else str(response['outage']['zzq7']).strip()):
                tmp = [str(document['zzQ7']).strip(), str(response['outage']['zzq7']).strip()]
                tmp.append("Incorrect value for 'zzQ7'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ8']).strip()) == ("" if response['outage']['zzq8'] is None else str(response['outage']['zzq8']).strip()):
                tmp = [str(document['zzQ8']).strip(), str(response['outage']['zzq8']).strip()]
                tmp.append("Incorrect value for 'zzQ8'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ9']).strip()) == ("" if response['outage']['zzq9'] is None else str(response['outage']['zzq9']).strip()):
                tmp = [str(document['zzQ9']).strip(), str(response['outage']['zzq9']).strip()]
                tmp.append("Incorrect value for 'zzQ9'!")
                table.append(tmp)
                status = 1

            if not (str(document['zzQ10']).strip()) == ("" if response['outage']['zzq10'] is None else str(response['outage']['zzq10']).strip()):
                tmp = [str(document['zzQ10']).strip(), str(response['outage']['zzq10']).strip()]
                tmp.append("Incorrect value for 'zzQ10'!")
                table.append(tmp)
                status = 1
                row.append("No Match Found")

            if not (str(document['ccList']).strip()== ("" if response['outage']['ccCustomer'] is None else str(response['outage']['ccCustomer']).strip())):
                tmp = [str(document['ccList']).strip(), str(response['outage']['ccCustomer']).strip()]
                tmp.append("Incorrect value for 'ccList/ccCustomer'!")
                table.append(tmp)
                status = 1

            if not (str(document['employeeEmail']).strip()== ("" if response['empEmailId'] is None else str(response['empEmailId']).strip())):
                tmp = [str(document['employeeEmail']).strip(), str(response['empEmailId']).strip()]
                tmp.append("Incorrect value for 'employeeEmail/empEmailId'!")
                table.append(tmp)
                status = 1

            if not (str(document['employeeId']).strip()== ("" if response['empId'] is None else str(response['empId']).strip())):
                tmp = [str(document['employeeId']).strip(), str(response['empId']).strip()]
                tmp.append("Incorrect value for 'employeeId/empId'!")
                table.append(tmp)
                status = 1

            if not (str(document['internalUse']).strip()== ("" if response['outage']['internalUse'] is None else str(response['outage']['internalUse']).strip())):
                tmp = [str(document['internalUse']).strip(), str(response['outage']['internalUse']).strip()]
                tmp.append("Incorrect value for 'internalUse/internalUse'!")
                table.append(tmp)
                status = 1

            if not (str(document['numberOfSystemsAffected']).strip()== ("" if response['outage']['numOfSystemsAffected'] is None else str(response['outage']['numOfSystemsAffected']).strip())):
                tmp = [str(document['numberOfSystemsAffected']).strip(), str(response['outage']['numOfSystemsAffected']).strip()]
                tmp.append("Incorrect value for 'numberOfSystemsAffected/numOfSystemsAffected'!")
                table.append(tmp)
                status = 1

            if not (str(document['numberOfUsersAffected']).strip()== ("" if response['outage']['numOfUsersAffected'] is None else str(response['outage']['numOfUsersAffected']).strip())):
                tmp = [str(document['numberOfUsersAffected']).strip(), str(response['outage']['numOfUsersAffected']).strip()]
                tmp.append("Incorrect value for 'numberOfUsersAffected/numOfUsersAffected'!")
                table.append(tmp)
                status = 1

            if not (str(document['technicalCategory1']).strip()== ("" if response['prodSeriesTech'] is None else str(response['prodSeriesTech']).strip())):
                tmp = [str(document['technicalCategory1']).strip(), str(response['prodSeriesTech']).strip()]
                tmp.append("Incorrect value for 'technicalCategory1/prodSeriesTech'!")
                table.append(tmp)
                status = 1

            if not (str(document['serviceProduct']).strip()== ("" if response['entitlement']['serviceProduct'] is None else str(response['entitlement']['serviceProduct']).strip())):
                tmp = [str(document['serviceProduct']).strip(), str(response['entitlement']['serviceProduct']).strip()]
                tmp.append("Incorrect value for 'serviceProduct/serviceProduct'!")
                table.append(tmp)
                status = 1

            print "\n\n##############################################"
            print "\tMatching Dates details...."
            print "##############################################\n\n"
            print "Number of srDates in document: "+str(len(document['srReqDate']))
            print "Number of Dates in API response: "+str(len(response['dates']))
            if document['srReqDate']:
                if response['dates']:
                    srReqDate = {'d': document['srReqDate']}
                    for srd in srReqDate['d']:
                        if srd['duration'] == "":
                            srd['duration'] = None
                        if srd['timeUnit'] == "":
                            srd['timeUnit'] = None
                        if srd['dateStamp'] != "":
                            d = str(srd['dateStamp'])
                            yy = d[:4]
                            mm = d[4:6]
                            dd = d[6:8]
                            hh = d[8:10]
                            mmm = d[10:12]
                            ss = d[12:]
                            srd['dateStamp'] = str(yy+"-"+mm+"-"+dd+" "+hh+":"+mmm+":"+ss)
                    dates = {'d': response['dates']}
                    for srd in srReqDate['d']:
                        match_level = 0
                        found = 0
                        match_location = 0
                        counter = 0
                        old_match_level = 0
                        match_data = ""
                        for d in dates['d']:
                            match_level = 0
                            match_data = ""
                            if ("" if srd['duration'] is None else srd['duration']) == ("" if d['duration'] is None else d['duration']):
                                match_level += 1
                            if ("" if srd['dateStamp'] is None else srd['dateStamp']) == ("" if d['dateStamp'] is None else d['dateStamp']):
                                match_level += 1
                            if ("" if srd['dateType'] is None else srd['dateType']) == ("" if d['dateType'] is None else d['dateType']):
                                match_level += 1
                            if ("" if srd['timeUnit'] is None else srd['timeUnit']) == ("" if d['timeUnit'] is None else d['timeUnit']):
                                if match_level >= 3:
                                    found = 1
                                    match_level += 1
                                    match_data = srd
                                    break;
                            if match_level >= old_match_level:
                                match_location = counter
                                old_match_level = match_level
                            counter += 1
                        if found == 0:
                            #print "************************************************"
                            print "Dates Data Mismatch, max number of values matched is "+str(old_match_level)
                            print "Kafka ==> "+str(json.dumps(srd, sort_keys=True))
                            print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                            tmp = ["", "", "Incorrect value for 'srDate'!"]
                            table.append(tmp)
                            status = 1
                            print "************************************************"
                        else:
                            #print "************************************************"
                            print "Dates Data matched, highest level of match is "+str(match_level)+". Data is "+str(json.dumps(srd))
                            #print "\tKafak ==> "+str(json.dumps(srd, sort_keys=True))
                            #print "\tAPI   ==> "+str(json.dumps(match_data, sort_keys=True))
                            print "************************************************"
                else:
                    print "No dates found in API response, but available in Kafka message."
                    print "Kafka Message: "+str(json.dumps(document['srReqDate']))
            else:
                print "No dates found in Kafka message."

            for pf in document['partnerFunction']:
                if str(pf['partnerName']).strip() == "":
                    pf['partnerName'] = None

            print "\n\n##############################################"
            print "\tMatching Partner Functions details...."
            print "##############################################\n\n"
            print "Number of PartnerFunction in document: "+str(len(document['partnerFunction']))
            print "Number of PartnerFunction in API response: "+str(len(response['partnerFunctions']))

            if document['partnerFunction']:
                if response['partnerFunctions']:
                    for pf in document['partnerFunction']:
                        match_level = 0
                        found = 0
                        match_location = 0
                        counter = 0
                        old_match_level = 0
                        match_data = ""
                        for pf2 in response['partnerFunctions']:
                            match_level = 0
                            match_data = ""
                            if ("" if pf['partnerFunctionName'] is None else pf['partnerFunctionName']) == ("" if pf2['partnerFunctionName'] is None else pf2['partnerFunctionName']):
                                match_level += 1
                            if ("" if pf['partnerFunctionKey'] is None else pf['partnerFunctionKey']) == ("" if pf2['partnerFunctionKey'] is None else pf2['partnerFunctionKey']):
                                match_level += 1
                            if str(("" if pf['partnerId'] is None else pf['partnerId'])) == str(("" if pf2['partnerID'] is None else pf2['partnerID'])):
                                match_level += 1
                            if ("" if pf['partnerName'] is None else pf['partnerName']) == ("" if pf2['partnerName'] is None else pf2['partnerName']):
                                if match_level >= 3:
                                    found = 1
                                    match_level += 1
                                    match_data = pf2
                                    break;
                            if match_level >= old_match_level:
                                match_location = counter
                                old_match_level = match_level
                                match_data = pf2
                            counter += 1
                        if found == 0:
                            #print "************************************************"
                            print "ParnterFunction Data Mismatch, highest level of match is "+str(old_match_level)
                            print "Kafka ==> "+str(json.dumps(pf, sort_keys=True))
                            print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                            tmp = ["", "", "Incorrect value for 'PartnerFunction'!"]
                            table.append(tmp)
                            status = 1
                            print "************************************************"
                        else:
                            #print "************************************************"
                            print "PartnerFunction Data matched, highest level of match is "+str(match_level)+". Data is "+str(json.dumps(pf))
                            #print "Kafka ==> "+str(json.dumps(pf, sort_keys=True))
                            #print "API   ==> "+str(json.dumps(match_data, sort_keys=True))
                            print "************************************************"
                else:
                    print "No partners found in API response, but available in Kafka message."
                    print "Kafka Message: "+str(json.dumps(document['partnerFunction']))
            else:
                print "No partners found in Kafka message."

            if status==0:
                print "Match Found"
                row.append("Match Found")
            else:
                #print "\nCompared JSONs"
                #print "Kafka: "+str(document)
                #print "API: "+str(json.dumps(response, sort_keys=True))
                print tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            print "No Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        return row

client = MongoClient('10.219.48.134', 27017)
#client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['srDetails']
api = HBase()
document_no = 0
ofile = open('Kafka_Verify_srDetails.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
row = ["SNo", "CaseID", "KafkaJSON", "APIResponse", "Status"]
writer.writerow(row)
cases = ['2015-1118-T-0426', '2015-1118-T-0427', '2015-1118-T-0428', '2015-1118-T-0431', '2015-1118-T-0429', '2015-1118-T-0432', '2015-1118-T-0436', '2015-1118-T-0430', '2015-1118-T-0434', '2015-1118-T-0440', '2015-1118-T-0433', '2015-1118-T-0437', '2015-1118-T-0438', '2015-1118-T-0442', '2015-1118-T-0441', '2015-1118-T-0435', '2015-1118-T-0445', '2015-1118-T-0439', '2015-1118-T-0451', '2015-1118-T-0450', '2015-1118-T-0446', '2015-1118-T-0444', '2015-1118-T-0443', '2015-1118-T-0448', '2015-1118-T-0449', '2015-1118-T-0447', '2015-1118-T-0454', '2015-1118-T-0460', '2015-1118-T-0461', '2015-1118-T-0452', '2015-1118-T-0456', '2015-1118-T-0459', '2015-1118-T-0462', '2015-1118-T-0457', '2015-1118-T-0455', '2015-1118-T-0458', '2015-1118-T-0468', '2015-1118-T-0465', '2015-1118-T-0466', '2015-1118-T-0472', '2015-1118-T-0453', '2015-1118-T-0475', '2015-1118-T-0473', '2015-1118-T-0469', '2015-1118-T-0476', '2015-1118-T-0467', '2015-1118-T-0474', '2015-1118-T-0470', '2015-1118-T-0464', '2015-1118-T-0463', '2015-1118-T-0471', '2015-1118-T-0477', '2015-1118-T-0478', '2015-1118-T-0480', '2015-1118-T-0479', '2015-1118-T-0481', '2015-1118-T-0482', '2015-1118-T-0483', '2015-1118-T-0485', '2015-1118-T-0486', '2015-1118-T-0487', '2015-1118-T-0484', '2015-1118-T-0491', '2015-1118-T-0488', '2015-1118-T-0489', '2015-1118-T-0490', '2015-1118-T-0497', '2015-1118-T-0492', '2015-1118-T-0498', '2015-1118-T-0496', '2015-1118-T-0493', '2015-1118-T-0499', '2015-1118-T-0495', '2015-1118-T-0506', '2015-1118-T-0502', '2015-1118-T-0503', '2015-1118-T-0500', '2015-1118-T-0508', '2015-1118-T-0494', '2015-1118-T-0501', '2015-1118-T-0507', '2015-1118-T-0504', '2015-1118-T-0509', '2015-1118-T-0520', '2015-1118-T-0515', '2015-1118-T-0510', '2015-1118-T-0514', '2015-1118-T-0517', '2015-1118-T-0513', '2015-1118-T-0505', '2015-1118-T-0511', '2015-1118-T-0519', '2015-1118-T-0521', '2015-1118-T-0516', '2015-1118-T-0512', '2015-1118-T-0523', '2015-1118-T-0522', '2015-1118-T-0518', '2015-1118-T-0524', '2015-1118-T-0527', '2015-1118-T-0525', '2015-1118-T-0526', '2015-1118-T-0528', '2015-1118-T-0529', '2015-1118-T-0533', '2015-1118-T-0531', '2015-1118-T-0532', '2015-1118-T-0530', '2015-1118-T-0535', '2015-1118-T-0537', '2015-1118-T-0536', '2015-1118-T-0534', '2015-1118-T-0538', '2015-1118-T-0539', '2015-1118-T-0541', '2015-1118-T-0546', '2015-1118-T-0543', '2015-1118-T-0545', '2015-1118-T-0542', '2015-1118-T-0549', '2015-1118-T-0547', '2015-1118-T-0544', '2015-1118-T-0548', '2015-1118-T-0551', '2015-1118-T-0550', '2015-1118-T-0555', '2015-1118-T-0561', '2015-1118-T-0554', '2015-1118-T-0553', '2015-1118-T-0559', '2015-1118-T-0557', '2015-1118-T-0558', '2015-1118-T-0540', '2015-1118-T-0552', '2015-1118-T-0560', '2015-1118-T-0562', '2015-1118-T-0556', '2015-1118-T-0564', '2015-1118-T-0563', '2015-1118-T-0566', '2015-1118-T-0570', '2015-1118-T-0569', '2015-1118-T-0565', '2015-1118-T-0567', '2015-1118-T-0568', '2015-1118-T-0575', '2015-1118-T-0574', '2015-1118-T-0573', '2015-1118-T-0572', '2015-1118-T-0571', '2015-1118-T-0577', '2015-1118-T-0576', '2015-1118-T-0579', '2015-1118-T-0578', '2015-1118-T-0581', '2015-1118-T-0580', '2015-1118-T-0583', '2015-1118-T-0584', '2015-1118-T-0582', '2015-1118-T-0586', '2015-1118-T-0585', '2015-1118-T-0587', '2015-1118-T-0590', '2015-1118-T-0588', '2015-1118-T-0592', '2015-1118-T-0591', '2015-1118-T-0589', '2015-1118-T-0593', '2015-1118-T-0597', '2015-1118-T-0596', '2015-1118-T-0595', '2015-1118-T-0602', '2015-1118-T-0594', '2015-1118-T-0599', '2015-1118-T-0600', '2015-1118-T-0604', '2015-1118-T-0605', '2015-1118-T-0601', '2015-1118-T-0598', '2015-1118-T-0606', '2015-1118-T-0607', '2015-1118-T-0603', '2015-1118-T-0611', '2015-1118-T-0612', '2015-1118-T-0608', '2015-1118-T-0613', '2015-1118-T-0614', '2015-1118-T-0617', '2015-1118-T-0609', '2015-1118-T-0616', '2015-1118-T-0610', '2015-1118-T-0615', '2015-1118-T-0619', '2015-1118-T-0618', '2015-1118-T-0621', '2015-1118-T-0623', '2015-1118-T-0620', '2015-1118-T-0622']
cases = []
for i in range(0, 100):
    cases.append("SPARKDOWN"+str(i))
print cases
cases_data = []
for c in cases:
    document = collection.find_one({'caseId': c})
    #print document
    if document:
        row = []
        document_no += 1
        row.append(document_no)
        row.append(document['caseId'])
        row.append("")
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
#print cases_data



