import requests
import json
import datetime
import time
from tabulate import tabulate
from colorama import init
init(autoreset=True)
__author__ = 'asifj'

class Utils:
    def __init__(self):
        self.url = "http://172.22.147.248:8092/api/"
        pass

    def header(self, document, document_no, case_key):
        output = "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        output += "\nAPI URL: "+self.url+"case-manager/cases/"+str(document[case_key])+"\n"
        output += "\nDocument No: "+str(document_no)
        output += "\nObject _id: "+str(document['_id'])
        output += "\nCaseID: "+str(document[case_key])
        keys = len(document.keys())
        output += "\nKeys: "+str(keys)
        return output

    def request(self, document, case_key):
        r = requests.get(self.url+"case-manager/cases/"+str(document[case_key]))
        return r

    def footer(self, output):
        output += "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        output += "\n\n"
        return output

    def validate_sr_details(self, r, document, document_no, start_time, response_writer):
        row = []
        output = ""
        row.append(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'))
        row.append(document_no)
        row.append(document['SRID'])
        row.append("SR")
        row.append("")
        row.append(r.status_code)
        row.append(r.elapsed)
        status = 0
        response = ""
        if r.status_code == 200:
            response = json.loads(r.text)
            table = []
            if not (str(document['BETA_TYPE']).strip() == ("" if response['betaType'] is None else str(response['betaType']).strip())):
                tmp = [str(document['BETA_TYPE']).strip(), str(response['betaType']).strip()]
                tmp.append("Incorrect value for 'betaType'!")
                table.append(tmp)
                status = 1

            if not (str(document['BUILD']).strip() == ("" if response['outage']['build'] is None else str(response['outage']['build']).strip())):
                tmp = [str(document['build']).strip(), str(response['outage']['build']).strip()]
                tmp.append("Incorrect value for 'build'!")
                table.append(tmp)
                status = 1

            if not (str(document['CC_ENGINEER']).strip() == ("" if response['outage']['ccEngineer'] is None else str(response['outage']['ccEngineer']).strip())):
                tmp = [str(document['CC_ENGINEER']).strip(), str(response['outage']['ccEngineer']).strip()]
                tmp.append("Incorrect value for 'ccEngineer'!")
                table.append(tmp)
                status = 1

            if not (str(document['SRID']).strip() == ("" if response['srId'] is None else str(response['srId']).strip())):
                tmp = [str(document['SRID']).strip(), str(response['srId']).strip()]
                tmp.append("Incorrect value for 'caseId'!")
                table.append(tmp)
                status = 1

            if not (str(document['CONTRACT_ID']).strip() == ("" if response['entitlement']['contractId'] is None else str(response['entitlement']['contractId']).strip())):
                tmp = [str(document['CONTRACT_ID']).strip(), str(response['entitlement']['contractId']).strip()]
                tmp.append("Incorrect value for 'contractId'!")
                table.append(tmp)
                status = 1

            if not (str(document['CONTRACT_STATUS']).strip()== ("" if response['entitlement']['contractStatus'] is None else str(response['entitlement']['contractStatus']).strip())):
                tmp = [str(document['CONTRACT_STATUS']).strip(), str(response['entitlement']['contractStatus']).strip()]
                tmp.append("Incorrect value for 'contractStatus'!")
                table.append(tmp)
                status = 1

            if not (str(document['COUNTRY']).strip() == ("" if response['outage']['country'] is None else str(response['outage']['country']).strip())):
                tmp = [str(document['COUNTRY']).strip(), str(response['outage']['country']).strip()]
                tmp.append("Incorrect value for 'country'!")
                table.append(tmp)
                status = 1

            if not (str(document['COURTESY']).strip()== ("" if response['courtesy'] is None else str(response['courtesy']).strip())):
                tmp = [str(document['COURTESY']).strip(), str(response['courtesy']).strip()]
                tmp.append("Incorrect value for 'courtesyDescription/courtesy'!")
                table.append(tmp)
                status = 1

            if not (str(document['COURTESY_KEY']).strip()== ("" if response['courtesyKey'] is None else str(response['courtesyKey']).strip())):
                tmp = [str(document['COURTESY_KEY']).strip(), str(response['courtesyKey']).strip()]
                tmp.append("Incorrect value for 'courtesykey'!")
                table.append(tmp)
                status = 1

            if not (str(document['CRITICAL_ISSUE']).strip()== ("" if response['outage']['criticalIssue'] is None else str(response['outage']['criticalIssue']).strip())):
                tmp = [str(document['CRITICAL_ISSUE']).strip(), str(response['outage']['criticalIssue']).strip()]
                tmp.append("Incorrect value for 'criticalIssue'!")
                table.append(tmp)
                status = 1

            if not (str(document['CRITICAL_OUTAGE']).strip()== ("" if response['criticalOutage'] is None else str(response['criticalOutage']).strip())):
                tmp = [str(document['CRITICAL_OUTAGE']).strip(), str(response['criticalOutage']).strip()]
                tmp.append("Incorrect value for 'criticalOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['CUST_CASE_NO']).strip()== ("" if response['outage']['custCaseNo'] is None else str(response['outage']['custCaseNo']).strip())):
                tmp = [str(document['CUST_CASE_NO']).strip(), str(response['outage']['custCaseNo']).strip()]
                tmp.append("Incorrect value for 'customerCaseNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['CVE']).strip()== ("" if response['cve'] is None else str(response['cve']).strip())):
                tmp = [str(document['CVE']).strip(), str(response['cve']).strip()]
                tmp.append("Incorrect value for 'cve'!")
                table.append(tmp)
                status = 1

            if not (str(document['CVSS']).strip()== ("" if response['cvss'] is None else str(response['cvss']).strip())):
                tmp = [str(document['CVSS']).strip(), str(response['cvss']).strip()]
                tmp.append("Incorrect value for 'cvss'!")
                table.append(tmp)
                status = 1

            if not (str(document['DESCRIPTION']).strip()== ("" if response['desc'] is None else str(response['desc']).strip())):
                tmp = [str(document['DESCRIPTION']).strip(), str(response['desc']).strip()]
                tmp.append("Incorrect value for 'description'!")
                table.append(tmp)
                status = 1

            if not (str(document['END_DATE']).strip() == ("" if response['entitlement']['endDate'] is None else str(response['entitlement']['endDate']).strip())):
                tmp = [str(document['END_DATE']).strip(), str(response['entitlement']['endDate']).strip()]
                tmp.append("Incorrect value for 'endDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['ENTITLED_SERIAL_NO']).strip()== ("" if response['entitlement']['entitledSerialNumber'] is None else str(response['entitlement']['entitledSerialNumber']).strip())):
                tmp = [str(document['ENTITLED_SERIAL_NO']).strip(), str(response['entitlement']['entitledSerialNumber']).strip()]
                tmp.append("Incorrect value for 'entitledSerialNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document['ENTITLEMENT_CHECKED']).strip()== ("" if response['entitlement']['entitlementChecked'] is None else str(response['entitlement']['entitlementChecked']).strip())):
                tmp = [str(document['ENTITLEMENT_CHECKED']).strip(), str(response['entitlement']['entitlementChecked']).strip()]
                tmp.append("Incorrect value for 'entitlementChecked'!")
                table.append(tmp)
                status = 1

            if not (str(document['ENTITLEMENT_SERVICE_LEVEL']).strip()== ("" if response['entitlement']['entitlementServiceLevel'] is None else str(response['entitlement']['entitlementServiceLevel']).strip())):
                tmp = [str(document['ENTITLEMENT_SERVICE_LEVEL']).strip(), str(response['entitlement']['entitlementServiceLevel']).strip()]
                tmp.append("Incorrect value for 'entitlementServiceLevel'!")
                table.append(tmp)
                status = 1

            if not (str(document['ENTITLEMENT_SOURCE']).strip()== ("" if response['entitlement']['entitlementSource'] is None else str(response['entitlement']['entitlementSource']).strip())):
                tmp = [str(document['ENTITLEMENT_SOURCE']).strip(), str(response['entitlement']['entitlementSource']).strip()]
                tmp.append("Incorrect value for 'entitlementSource'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('ESCALATION_DES', '')).strip()== ("" if response['escalationDesc'] is None else str(response['escalationDesc']).strip())):
                tmp = [str(document['escalation']).strip(), str(response['escalationDesc']).strip()]
                tmp.append("Incorrect value for 'escalation'!")
                table.append(tmp)
                status = 1

            if not (str(document['ESCALATION_LEVEL']).strip()== ("" if response['outage']['escalationLevel'] is None else str(response['outage']['escalationLevel']).strip())):
                tmp = [str(document['escalationLevelDescription']).strip(), str(response['outage']['escalationLevel']).strip()]
                tmp.append("Incorrect value for 'escalationLevelDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['ESCALATION_LEVEL_KEY']).strip()== ("" if response['outage']['escalationLevelkey'] is None else str(response['outage']['escalationLevelkey']).strip())):
                tmp = [str(document['ESCALATION_LEVEL_KEY']).strip(), str(response['outage']['escalationLevelkey']).strip()]
                tmp.append("Incorrect value for 'escalationLevelKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['ESCALATION_KEY']).strip()== ("" if response['escalationKey'] is None else str(response['escalationKey']).strip())):
                tmp = [str(document['ESCALATION_KEY']).strip(), str(response['escalationKey']).strip()]
                tmp.append("Incorrect value for 'escalationkey'!")
                table.append(tmp)
                status = 1

            if not (str(document['EXTERNALLY_REPORTED']).strip()== ("" if response['externallyReported'] is None else str(response['externallyReported']).strip())):
                tmp = [str(document['EXTERNALLY_REPORTED']).strip(), str(response['externallyReported']).strip()]
                tmp.append("Incorrect value for 'externallyReported'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('FOLLOW_UP_METHOD', '')).strip()== ("" if response['outage']['followUpMethod'] is None else str(response['outage']['followUpMethod']).strip())):
                tmp = [str(document['FOLLOW_UP_METHOD']).strip(), str(response['outage']['followUpMethod']).strip()]
                tmp.append("Incorrect value for 'followupMethod'!")
                table.append(tmp)
                status = 1

            if not (str(document['FOLLOW_UP_METHOD_KEY']).strip()== ("" if response['outage']['followUpMethodkey'] is None else str(response['outage']['followUpMethodkey']).strip())):
                tmp = [str(document['FOLLOW_UP_METHOD_KEY']).strip(), str(response['outage']['followUpMethodkey']).strip()]
                tmp.append("Incorrect value for 'followupMethodKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['JSA_ADVISORY_BOARD']).strip()== ("" if response['jsaAdvisoryBoard'] is None else str(response['jsaAdvisoryBoard']).strip())):
                tmp = [str(document['JSA_ADVISORY_BOARD']).strip(), str(response['jsaAdvisoryBoard']).strip()]
                tmp.append("Incorrect value for 'jsaAdvisoryBoard'!")
                table.append(tmp)
                status = 1

            if not (str(document['JTAC']).strip()== ("" if response['jtac'] is None else str(response['jtac']).strip())):
                tmp = [str(document['JTAC']).strip(), str(response['jtac']).strip()]
                tmp.append("Incorrect value for 'jtac'!")
                table.append(tmp)
                status = 1

            if not (str(document['KNOWLEDGE_ARTICLE']).strip()== ("" if response['outage']['knowledgeArticle'] is None else str(response['outage']['knowledgeArticle']).strip())):
                tmp = [str(document['KNOWLEDGE_ARTICLE']).strip(), str(response['outage']['knowledgeArticle']).strip()]
                tmp.append("Incorrect value for 'knowledgeArticle'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUATGE_CAUSE']).strip()== ("" if response['outage']['outageCause'] is None else str(response['outage']['outageCause']).strip())):
                tmp = [str(document['OUATGE_CAUSE']).strip(), str(response['outage']['outageCause']).strip()]
                tmp.append("Incorrect value for 'OUATGE_CAUSE/outageCause'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_CAUSE_KEY']).strip()== ("" if response['outage']['outageCausekey'] is None else str(response['outage']['outageCausekey']).strip())):
                tmp = [str(document['OUTAGE_CAUSE_KEY']).strip(), str(response['outage']['outageCausekey']).strip()]
                tmp.append("Incorrect value for 'outageCauseKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE']).strip()== ("" if response['outage']['outage'] is None else str(response['outage']['outage']).strip())):
                tmp = [str(document['OUTAGE']).strip(), str(response['outage']['outage']).strip()]
                tmp.append("Incorrect value for 'outageDescription/outage'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_IMPACT_KEY']).strip()== ("" if response['outage']['outageImpactKey'] is None else str(response['outage']['outageImpactKey']).strip())):
                tmp = [str(document['OUTAGE_IMPACT_KEY']).strip(), str(response['outage']['outageImpactKey']).strip()]
                tmp.append("Incorrect value for 'outageImpactKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_INFO_AVAILABLE']).strip()== ("" if response['outage']['outageInfoAvailable'] is None else str(response['outage']['outageInfoAvailable']).strip())):
                tmp = [str(document['OUTAGE_INFO_AVAILABLE']).strip(), str(response['outage']['outageInfoAvailable']).strip()]
                tmp.append("Incorrect value for 'outageInfoAvailable'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_KEY']).strip()== ("" if response['outage']['outageKey'] is None else str(response['outage']['outageKey']).strip())):
                tmp = [str(document['OUTAGE_KEY']).strip(), str(response['outage']['outageKey']).strip()]
                tmp.append("Incorrect value for 'outageKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_TYPE']).strip()== ("" if response['outage']['outageType'] is None else str(response['outage']['outageType']).strip())):
                tmp = [str(document['OUTAGE_TYPE']).strip(), str(response['outage']['outageType']).strip()]
                tmp.append("Incorrect value for 'outageTypeDescription/outageType'!")
                table.append(tmp)
                status = 1

            if not (str(document['OUTAGE_TYPE_KEY']).strip()== ("" if response['outage']['outageTypekey'] is None else str(response['outage']['outageTypekey']).strip())):
                tmp = [str(document['OUTAGE_TYPE_KEY']).strip(), str(response['outage']['outageTypekey']).strip(),
                       "Incorrect value for 'outageTypeKey'!"]
                table.append(tmp)
                status = 1

            if not (str(document['OUTSOURCER']).strip()== ("" if response['outage']['outsourcer'] is None else str(response['outage']['outsourcer']).strip())):
                tmp = [str(document['OUTSOURCER']).strip(), str(response['outage']['outsourcer']).strip(),
                       "Incorrect value for 'outsourcer'!"]
                table.append(tmp)
                status = 1

            if not (str(document['OVERIDE_OUTAGE']).strip()== ("" if response['outage']['overideOutage'] is None else str(response['outage']['overideOutage']).strip())):
                tmp = [str(document['OVERIDE_OUTAGE']).strip(), str(response['outage']['overideOutage']).strip()]
                tmp.append("Incorrect value for 'overideOutage'!")
                table.append(tmp)
                status = 1

            if not (str(document['PLATFORM']).strip()== ("" if response['platform'] is None else str(response['platform']).strip())):
                tmp = [str(document['PLATFORM']).strip(), str(response['platform']).strip()]
                tmp.append("Incorrect value for 'platform'!")
                table.append(tmp)
                status = 1

            if not (str(document['PREVIOUS_OWNER_SKILL']).strip()== ("" if response['outage']['previousOwnerSkill'] is None else str(response['outage']['previousOwnerSkill']).strip())):
                tmp = [str(document['PREVIOUS_OWNER_SKILL']).strip(), str(response['outage']['previousOwnerSkill']).strip()]
                tmp.append("Incorrect value for 'previousOwnerSkill'!")
                table.append(tmp)
                status = 1

            if not (str(document['PREVIOUS_TEAM']).strip()== ("" if response['outage']['previousTeam'] is None else str(response['outage']['previousTeam']).strip())):
                tmp = [str(document['PREVIOUS_TEAM']).strip(), str(response['outage']['previousTeam']).strip()]
                tmp.append("Incorrect value for 'previousTeam'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('PRIORITY', '')).strip()== ("" if response['priority'] is None else str(response['priority']).strip())):
                tmp = [str(document['PRIORITY']).strip(), str(response['priority']).strip()]
                tmp.append("Incorrect value for 'priority'!")
                table.append(tmp)
                status = 1

            if not (str(document['PRIORITY_KEY']).strip()== ("" if response['priorityKey'] is None else str(response['priorityKey']).strip())):
                tmp = [str(document['PRIORITY_KEY']).strip(), str(response['priorityKey']).strip()]
                tmp.append("Incorrect value for 'priorityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['PROCESS_TYPE']).strip()== ("" if response['processType'] is None else str(response['processType']).strip())):
                tmp = [str(document['PROCESS_TYPE']).strip(), str(response['processType']).strip()]
                tmp.append("Incorrect value for 'processType'!")
                table.append(tmp)
                status = 1

            if not (str(document['PROCESS_TYPE_DES']).strip()== ("" if response['processTypeDesc'] is None else str(response['processTypeDesc']).strip())):
                tmp = [str(document['PROCESS_TYPE_DES']).strip(), str(response['processTypeDesc']).strip()]
                tmp.append("Incorrect value for 'processTypeDescription'!")
                table.append(tmp)
                status = 1

            if not (str(document['PRODUCT_ID']).strip() == ("" if response['productId'] is None else str(response['productId']).strip())):
                tmp = [str(document['PRODUCT_ID']).strip(), str(response['productId']).strip()]
                tmp.append("Incorrect value for 'productId'!")
                table.append(tmp)
                status = 1

            if not (str(document['PRODUCT_SERIES']).strip() == ("" if response['productSeries'] is None else str(response['productSeries']).strip())):
                tmp = [str(document['PRODUCT_SERIES']).strip(), str(response['productSeries']).strip()]
                tmp.append("Incorrect value for 'productSeries'!")
                table.append(tmp)
                status = 1

            if not (str(document['RA_FA']).strip()== ("" if response['outage']['raFa'] is None else str(response['outage']['raFa']).strip())):
                tmp = [str(document['RA_FA']).strip(), str(response['outage']['raFa']).strip()]
                tmp.append("Incorrect value for 'raFa'!")
                table.append(tmp)
                status = 1

            if not (str(document['REASON']).strip()== ("" if response['reason'] is None else str(response['reason']).strip())):
                tmp = [str(document['REASON']).strip(), str(response['reason']).strip()]
                tmp.append("Incorrect value for 'reason'!")
                table.append(tmp)
                status = 1

            if not (str(document['RELEASE']).strip()== ("" if response['release'] is None else str(response['release']).strip())):
                tmp = [str(document['RELEASE']).strip(), str(response['release']).strip()]
                tmp.append("Incorrect value for 'release'!")
                table.append(tmp)
                status = 1

            if not (str(document['REPORTER_DETAILS']).strip()== ("" if response['reporterDetails'] is None else str(response['reporterDetails']).strip())):
                tmp = [str(document['REPORTER_DETAILS']).strip(), str(response['reporterDetails']).strip()]
                tmp.append("Incorrect value for 'reporterDetails'!")
                table.append(tmp)
                status = 1

            if not (str(document['ROUTER_NAME']).strip()== ("" if response['outage']['routerName'] is None else str(response['outage']['routerName']).strip())):
                tmp = [str(document['ROUTER_NAME']).strip(), str(response['outage']['routerName']).strip()]
                tmp.append("Incorrect value for 'routerName'!")
                table.append(tmp)
                status = 1

            if not (str(document['SEC_VULNERABILITY']).strip()== ("" if response['secVulnerability'] is None else str(response['secVulnerability']).strip())):
                tmp = [str(document['SEC_VULNERABILITY']).strip(), str(response['secVulnerability']).strip()]
                tmp.append("Incorrect value for 'secVulnerability'!")
                table.append(tmp)
                status = 1

            if not (str(document['SERIAL_NUMBER']).strip()== ("" if response['serialNumber'] is None else str(response['serialNumber']).strip())):
                tmp = [str(document['SERIAL_NUMBER']).strip(), str(response['serialNumber']).strip()]
                tmp.append("Incorrect value for 'serialNumber'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('SEVERITY', '')).strip()== ("" if response['severity'] is None else str(response['severity']).strip())):
                tmp = [str(document['SEVERITY']).strip(), str(response['severity']).strip()]
                tmp.append("Incorrect value for 'severity'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('SEVERITY_KEY', '')).strip()== ("" if response['severityKey'] is None else str(response['severityKey']).strip())):
                tmp = [str(document['SEVERITY_KEY']).strip(), str(response['severityKey']).strip()]
                tmp.append("Incorrect value for 'severityKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['SIRT_BUNDLE']).strip()== ("" if response['sirtBundle'] is None else str(response['sirtBundle']).strip())):
                tmp = [str(document['SIRT_BUNDLE']).strip(), str(response['sirtBundle']).strip()]
                tmp.append("Incorrect value for 'sirtBundle'!")
                table.append(tmp)
                status = 1

            if not (str(document['SKU']).strip()== ("" if response['entitlement']['sku'] is None else str(response['entitlement']['sku']).strip())):
                tmp = [str(document['SKU']).strip(), str(response['entitlement']['sku']).strip()]
                tmp.append("Incorrect value for 'sku'!")
                table.append(tmp)
                status = 1

            if not (str(document['SME_CONTACT']).strip()== ("" if response['smeContact'] is None else str(response['smeContact']).strip())):
                tmp = [str(document['SME_CONTACT']).strip(), str(response['smeContact']).strip()]
                tmp.append("Incorrect value for 'smeContact'!")
                table.append(tmp)
                status = 1

            if not (str(document['SOFTWARE']).strip()== ("" if response['software'] is None else str(response['software']).strip())):
                tmp = [str(document['SOFTWARE']).strip(), str(response['software']).strip()]
                tmp.append("Incorrect value for 'software'!")
                table.append(tmp)
                status = 1

            if not (str(document['SPECIAL_RELEASE']).strip()== ("" if response['specialRelease'] is None else str(response['specialRelease']).strip())):
                tmp = [str(document['SPECIAL_RELEASE']).strip(), str(response['specialRelease']).strip()]
                tmp.append("Incorrect value for 'specialRelease'!")
                table.append(tmp)
                status = 1

            if not (str(document['SR_CATEGORY1']).strip()== ("" if response['srCat1'] is None else str(response['srCat1']).strip())):
                tmp = [str(document['SR_CATEGORY1']).strip(), str(response['srCat1']).strip()]
                tmp.append("Incorrect value for 'srCategory1'!")
                table.append(tmp)
                status = 1

            if not (str(document['SR_CATEGORY2']).strip()== ("" if response['srCat2'] is None else str(response['srCat2']).strip())):
                tmp = [str(document['SR_CATEGORY2']).strip(), str(response['srCat2']).strip()]
                tmp.append("Incorrect value for 'srCategory2'!")
                table.append(tmp)
                status = 1

            if not (str(document['SR_CATEGORY3']).strip()== ("" if response['srCat3'] is None else str(response['srCat3']).strip())):
                tmp = [str(document['SR_CATEGORY3']).strip(), str(response['srCat3']).strip()]
                tmp.append("Incorrect value for 'srCategory3'!")
                table.append(tmp)
                status = 1

            if not (str(document['SR_CATEGORY4']).strip()== ("" if response['srCat4'] is None else str(response['srCat4']).strip())):
                tmp = [str(document['SR_CATEGORY4']).strip(), str(response['srCat4']).strip()]
                tmp.append("Incorrect value for 'srCategory4'!")
                table.append(tmp)
                status = 1

            if not (str(document['START_DATE']).strip()== ("" if response['entitlement']['startDate'] is None else str(response['entitlement']['startDate']).strip())):
                tmp = [str(document['START_DATE']).strip(), str(response['entitlement']['startDate']).strip()]
                tmp.append("Incorrect value for 'startDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['STATUS']).strip()== ("" if response['status'] is None else str(response['status']).strip())):
                tmp = [str(document['STATUS']).strip(), str(response['status']).strip()]
                tmp.append("Incorrect value for 'status'!")
                table.append(tmp)
                status = 1

            if not (str(document['STATUS_KEY']).strip()== ("" if response['statusKey'] is None else str(response['statusKey']).strip())):
                tmp = [str(document['STATUS_KEY']).strip(), str(response['statusKey']).strip()]
                tmp.append("Incorrect value for 'statusKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['TECHNICAL_CATEGORY1']).strip()== ("" if response['techCat1'] is None else str(response['techCat1']).strip())):
                tmp = [str(document['technicalCategory1']).strip(), str(response['techCat1']).strip()]
                tmp.append("Incorrect value for 'technicalCategory1'!")
                table.append(tmp)
                status = 1

            if not (str(document['TECHNICAL_CATEGORY2']).strip()== ("" if response['techCat2'] is None else str(response['techCat2']).strip())):
                tmp = [str(document['technicalCategory2']).strip(), str(response['techCat2']).strip()]
                tmp.append("Incorrect value for 'technicalCategory2'!")
                table.append(tmp)
                status = 1

            if not (str(document['TECHNICAL_CATEGORY3']).strip()== ("" if response['techCat3'] is None else str(response['techCat3']).strip())):
                tmp = [str(document['technicalCategory3']).strip(), str(response['techCat3']).strip()]
                tmp.append("Incorrect value for 'technicalCategory3'!")
                table.append(tmp)
                status = 1

            if not (str(document['TEMPERATURE']).strip()== ("" if response['outage']['temperature'] is None else str(response['outage']['temperature']).strip())):
                tmp = [str(document['TEMPERATURE']).strip(), str(response['outage']['temperature']).strip()]
                tmp.append("Incorrect value for 'temperature'!")
                table.append(tmp)
                status = 1

            if not (str(document['THEATER']).strip()== ("" if response['outage']['theater'] is None else str(response['outage']['theater']).strip())):
                tmp = [str(document['THEATER']).strip(), str(response['outage']['theater']).strip()]
                tmp.append("Incorrect value for 'theaterDescription/theater'!")
                table.append(tmp)
                status = 1

            if not (str(document['THEATER_KEY']).strip()== ("" if response['outage']['theaterkey'] is None else str(response['outage']['theaterkey']).strip())):
                tmp = [str(document['THEATER_KEY']).strip(), str(response['outage']['theaterkey']).strip()]
                tmp.append("Incorrect value for 'theaterKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['TOP5']).strip()== ("" if response['outage']['top5'] is None else str(response['outage']['top5']).strip())):
                tmp = [str(document['TOP5']).strip(), str(response['outage']['top5']).strip()]
                tmp.append("Incorrect value for 'top5'!")
                table.append(tmp)
                status = 1

            if not (str(document['TOTAL_OUTAGE_TIME']).strip()== ("" if response['outage']['totalOutageTime'] is None else str(response['outage']['totalOutageTime']).strip())):
                tmp = [str(document['TOTAL_OUTAGE_TIME']).strip(), str(response['outage']['totalOutageTime']).strip()]
                tmp.append("Incorrect value for 'totalOutageTime'!")
                table.append(tmp)
                status = 1

            if not (str(document.get('URGENCY', '')).strip()== ("" if response['urgency'] is None else str(response['urgency']).strip())):
                tmp = [str(document['URGENCY']).strip(), str(response['urgency']).strip()]
                tmp.append("Incorrect value for 'urgency'!")
                table.append(tmp)
                status = 1

            if not (str(document['URGENCY_KEY']).strip()== ("" if response['urgencyKey'] is None else str(response['urgencyKey']).strip())):
                tmp = [str(document['URGENCY_KEY']).strip(), str(response['urgencyKey']).strip()]
                tmp.append("Incorrect value for 'urgencyKey'!")
                table.append(tmp)
                status = 1

            if not (str(document['VERSION']).strip()== ("" if response['version'] is None else str(response['version']).strip())):
                tmp = [str(document['VERSION']).strip(), str(response['version']).strip()]
                tmp.append("Incorrect value for 'version'!")
                table.append(tmp)
                status = 1

            if not (str(document['VIA']).strip()== ("" if response['outage']['via'] is None else str(response['outage']['via']).strip())):
                tmp = [str(document['VIA']).strip(), str(response['via']).strip()]
                tmp.append("Incorrect value for 'viaDescription/via'!")
                table.append(tmp)
                status = 1

            if not (str(document['VIA_kEY']).strip()== ("" if response['outage']['viaKey'] is None else str(response['outage']['viaKey']).strip())):
                tmp = [str(document['VIA_kEY']).strip(), str(response['viaKey']).strip()]
                tmp.append("Incorrect value for 'VIA_kEY'!")
                table.append(tmp)
                status = 1

            if not (str(document['WARRANTY_END_DATE']).strip()== ("" if response['entitlement']['warrantyEndDate'] is None else str(response['entitlement']['warrantyEndDate']).strip())):
                tmp = [str(document['WARRANTY_END_DATE']).strip(), str(response['entitlement']['warrantyEndDate']).strip()]
                tmp.append("Incorrect value for 'warrantyEndDate'!")
                table.append(tmp)
                status = 1

            if not (str(document['SUPPORT_24_7']).strip()== ("" if response['outage']['support24X7'] is None else str(response['outage']['support24X7']).strip())):
                tmp = [str(document['SUPPORT_24_7']).strip(), str(response['outage']['support24X7']).strip()]
                tmp.append("Incorrect value for 'yearRoundSupport/support24X7'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ1']).strip()== ("" if response['outage']['zzq1'] is None else str(response['outage']['zzq1']).strip())):
                tmp = [str(document['ZZQ1']).strip(), str(response['outage']['zzq1']).strip()]
                tmp.append("Incorrect value for 'zzQ1'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ2']).strip()== ("" if response['outage']['zzq2'] is None else str(response['outage']['zzq2']).strip())):
                tmp = [str(document['ZZQ2']).strip(), str(response['outage']['zzq2']).strip()]
                tmp.append("Incorrect value for 'zzQ2'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ3']).strip()== ("" if response['outage']['zzq3'] is None else str(response['outage']['zzq3']).strip())):
                tmp = [str(document['ZZQ3']).strip(), str(response['outage']['zzq3']).strip()]
                tmp.append("Incorrect value for 'zzQ3'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ4']).strip()== ("" if response['outage']['zzq4'] is None else str(response['outage']['zzq4']).strip())):
                tmp = [str(document['ZZQ4']).strip(), str(response['outage']['zzq4']).strip()]
                tmp.append("Incorrect value for 'zzQ4'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ5']).strip()== ("" if response['outage']['zzq5'] is None else str(response['outage']['zzq5']).strip())):
                tmp = [str(document['ZZQ5']).strip(), str(response['outage']['zzq5']).strip()]
                tmp.append("Incorrect value for 'zzQ5'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ6']).strip()== ("" if response['outage']['zzq6'] is None else str(response['outage']['zzq6']).strip())):
                tmp = [str(document['ZZQ6']).strip(), str(response['outage']['zzq6']).strip()]
                tmp.append("Incorrect value for 'zzQ6'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ7']).strip()== ("" if response['outage']['zzq7'] is None else str(response['outage']['zzq7']).strip())):
                tmp = [str(document['ZZQ7']).strip(), str(response['outage']['zzq7']).strip()]
                tmp.append("Incorrect value for 'zzQ7'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ8']).strip()== ("" if response['outage']['zzq8'] is None else str(response['outage']['zzq8']).strip())):
                tmp = [str(document['ZZQ8']).strip(), str(response['outage']['zzq8']).strip()]
                tmp.append("Incorrect value for 'zzQ8'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ9']).strip()== ("" if response['outage']['zzq9'] is None else str(response['outage']['zzq9']).strip())):
                tmp = [str(document['ZZQ9']).strip(), str(response['outage']['zzq9']).strip()]
                tmp.append("Incorrect value for 'zzQ9'!")
                table.append(tmp)
                status = 1

            if not (str(document['ZZQ10']).strip()== ("" if response['outage']['zzq10'] is None else str(response['outage']['zzq10']).strip())):
                tmp = [str(document['ZZQ10']).strip(), str(response['outage']['zzq10']).strip()]
                tmp.append("Incorrect value for 'zzQ10'!")
                table.append(tmp)
                status = 1

            if not (str(document['CC_CUSTOMER']).strip()== ("" if response['outage']['ccCustomer'] is None else str(response['outage']['ccCustomer']).strip())):
                tmp = [str(document['CC_CUSTOMER']).strip(), str(response['outage']['ccCustomer']).strip()]
                tmp.append("Incorrect value for 'CC_CUSTOMER/ccCustomer'!")
                table.append(tmp)
                status = 1

            if not (str(document['EMP_MAIL_ID']).strip()== ("" if response['empEmailId'] is None else str(response['empEmailId']).strip())):
                tmp = [str(document['EMP_MAIL_ID']).strip(), str(response['empEmailId']).strip()]
                tmp.append("Incorrect value for 'EMP_MAIL_ID/empEmailId'!")
                table.append(tmp)
                status = 1

            if not (str(document['EMPID']).strip()== ("" if response['empId'] is None else str(response['empId']).strip())):
                tmp = [str(document['EMPID']).strip(), str(response['empId']).strip()]
                tmp.append("Incorrect value for 'EMPID/empId'!")
                table.append(tmp)
                status = 1

            if not (str(document['INTERNAL_USE']).strip()== ("" if response['outage']['internalUse'] is None else str(response['outage']['internalUse']).strip())):
                tmp = [str(document['INTERNAL_USE']).strip(), str(response['outage']['internalUse']).strip()]
                tmp.append("Incorrect value for 'INTERNAL_USE/internalUse'!")
                table.append(tmp)
                status = 1

            if not (str(document['NO_OF_SYSTEMS_AFFECTED']).strip()== ("" if response['outage']['numOfSystemsAffected'] is None else str(response['outage']['numOfSystemsAffected']).strip())):
                tmp = [str(document['NO_OF_SYSTEMS_AFFECTED']).strip(), str(response['outage']['numOfSystemsAffected']).strip()]
                tmp.append("Incorrect value for 'NO_OF_SYSTEMS_AFFECTED/numOfSystemsAffected'!")
                table.append(tmp)
                status = 1

            if not (str(document['NO_OF_USERS_AFFECTED']).strip()== ("" if response['outage']['numOfUsersAffected'] is None else str(response['outage']['numOfUsersAffected']).strip())):
                tmp = [str(document['NO_OF_USERS_AFFECTED']).strip(), str(response['outage']['numOfUsersAffected']).strip()]
                tmp.append("Incorrect value for 'NO_OF_USERS_AFFECTED/numOfUsersAffected'!")
                table.append(tmp)
                status = 1

            if not (str(document['PRODUCT_SERIES_TECH']).strip()== ("" if response['prodSeriesTech'] is None else str(response['prodSeriesTech']).strip())):
                tmp = [str(document['PRODUCT_SERIES_TECH']).strip(), str(response['prodSeriesTech']).strip()]
                tmp.append("Incorrect value for 'PRODUCT_SERIES_TECH/prodSeriesTech'!")
                table.append(tmp)
                status = 1

            if not (str(document['SERVICE_PRODUCT']).strip()== ("" if response['entitlement']['serviceProduct'] is None else str(response['entitlement']['serviceProduct']).strip())):
                tmp = [str(document['SERVICE_PRODUCT']).strip(), str(response['entitlement']['serviceProduct']).strip()]
                tmp.append("Incorrect value for 'SERVICE_PRODUCT/serviceProduct'!")
                table.append(tmp)
                status = 1

            if status==0:
                output += "\nMatch Found"
                row.append("Match Found")
            else:
                output += "\nData Mismatch\n"
                row.append("Data Mismatch")
                output += tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            output += "\nNo Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        totalTime = datetime.datetime.now() - start_time
        row.append(totalTime)
        response_writer.writerow(row)
        return output

    def validate_kb_links(self, r, document, document_no, start_time, response_writer):
        row = []
        output = ""
        row.append(document_no)
        row.append(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'))
        row.append(document['caseId'])
        row.append("KBLINKS")
        row.append("")
        row.append(r.status_code)
        row.append(r.elapsed)
        status = 0
        response = ""
        if r.status_code == 200:
            response = json.loads(r.text)
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                print "Incorrect value for 'caseId'!"
                status = 1
            document_kbLinks_len = len(document['link'])
            if type(document['link']) is dict:
                output += "kBLinks in document is not an array!"
                document_kbLinks_len = 1
                document['link'] =  [document['link']]
            if response['kbLinks'] is not None:
                response_kbLinks_len = len(response['kbLinks'])
            else:
                response_kbLinks_len = 0

            output += "\nNumber of kbLinks in document: "+str(document_kbLinks_len)
            output += "\nNumber of kbLinks in API response: "+str(response_kbLinks_len)

            if document_kbLinks_len==0:
                output += "No kbLinks found in document!"
                row.append("No kbLinks found in document!")
                output += "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                output += "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                response_writer(row)
                return output
            if response_kbLinks_len==0 and document_kbLinks_len>0:
                output += "No kbLinks found in API response but present in document."
                row.append("No kbLinks found in API response but present in document.")
                output += "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                output += "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                response_writer(row)
                return output

            for doc_link in document['link']:
                match_level = 0
                found = 0
                match_location = 0
                counter = 0
                old_match_level = 0
                match_data = ""
                for resp in response['kbLinks']:
                    match_level = 0
                    if str(doc_link['KBID']).strip() == str(("" if resp['kbId'] is None else resp['kbId'])).strip():
                        match_level += 1
                    if str(doc_link['STATUS']).strip() == str(("" if resp['status'] is None else resp['status'])).strip():
                        match_level += 1
                    if str(doc_link['DESCRIPTION']).strip() == str(("" if resp['description'] is None else resp['description'])).strip():
                        match_level += 1
                    if str(doc_link['INTERNALID']).strip() == str(("" if resp['internalId'] is None else resp['internalId'])).strip():
                        match_level += 1
                    if str(doc_link['URL']).strip() == str(("" if resp['url'] is None else resp['url'])).strip():
                        match_level += 1
                    if str("" if doc_link['KBDATE']=="0" else doc_link['KBDATE']).strip() == str(("" if resp['kbDate'] is None else resp['kbDate'])).strip().replace("-", "").replace(":", "").replace(" ", ""):
                        match_level += 1
                    if str(doc_link['DATA_SOURCE']).strip() == str(("" if resp['dataSource'] is None else resp['dataSource'])).strip():
                        match_level += 1
                    if str(doc_link['SOURCEVISIBILITY']).strip() == str(("" if resp['srcVisiblity'] is None else resp['srcVisiblity'])).strip():
                        match_level += 1
                    if str(doc_link['KB_FLAG']).strip() == str(("" if resp['kbFlag'] is None else resp['kbFlag'])).strip():
                        match_level += 1
                    if str(doc_link['SRVISIBILITY']).strip() == str(("" if resp['srVisibility'] is None else resp['srVisibility'])).strip():
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
                    output += "\n************************************************"
                    output += "\nData Mismatch, max number of values matched is "+str(old_match_level)
                    output += "\nKafka ==> "+str(json.dumps(doc_link, sort_keys=True))
                    output += "\nAPI   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", "", "Incorrect value for 'kbLinks'!"]
                    table.append(tmp)
                    status = 1
                    output += "\n************************************************"
                else:
                    output += "\nData matched, highest level of match is "+str(match_level)
                    output += "\nKafka ==> "+str(json.dumps(doc_link, sort_keys=True))
                    output += "\nAPI   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", "", "Match found for 'kbLinks'!"]
                    table.append(tmp)

            if status == 0:
                output += "\nMatch Found"
                row.append("Match Found")
            else:
                row.append("Data Mismatch")
                output += "\nCompared JSONs"
                output += "Kafka: "+str(json.dumps(document['link'], sort_keys=True))
                output += "API: "+str(json.dumps(response['kbLinks'], sort_keys=True))
                output += "\n"
                output += tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            output += "\nNo Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        totalTime = datetime.datetime.now() - start_time
        row.append(totalTime)
        response_writer.writerow(row)
        return output

    def validate_sr_attachments(self, r, document, document_no, start_time, response_writer):
        row = []
        output = ""
        row.append(document_no)
        row.append(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'))
        row.append(document['caseId'])
        row.append("Attachments")
        row.append("")
        row.append(r.status_code)
        row.append(r.elapsed)
        status = 0
        response = ""
        if r.status_code==200:
            response = json.loads(r.text)
            table = []
            if not (str(document['caseId']).strip() == "" if response['srId'] is None else str(response['srId']).strip()):
                output += "Incorrect value for 'caseId'!"
                status = 1
            if response['attachments'] is not None:
                response_attachment_len = len(response['attachments'])
            else:
                response_attachment_len = 0
            document_attachment_len = len(document['attachment'])
            if type(document['attachment']) is dict:
                output += "attachment in document is not an array!"
                document_attachment_len = 1
                document['attachment'] =  [document['attachment']]

            output += "\nNumber of attachments in document: "+str(document_attachment_len)
            output += "\nNumber of attachments in API response: "+str(response_attachment_len)

            if document_attachment_len==0:
                output += "\nNo attachment found in document!"
                row.append("No attachment found in document!")
                output += "Kafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                output += "API: "+str(json.dumps(response['attachments'], sort_keys=True))
                response_writer(row)
                return output
            if response_attachment_len==0 and document_attachment_len>0:
                output += "\nNo attachment found in API response but present in document."
                row.append("No attachment found in API response but present in document.")
                output += "Kafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                output += "API: "+str(json.dumps(response['attachments'], sort_keys=True))
                response_writer(row)
                return output

            for doc_attachment in document['attachment']:
                match_level = 0
                found = 0
                match_location = 0
                counter = 0
                old_match_level = 0
                zDate = ""
                if int(doc_attachment['ZDATE']) == 0:
                    zDate = ""
                else:
                    zDate = str(doc_attachment['ZDATE'])
                    zDate = zDate[:4]+"-"+zDate[4:6]+"-"+zDate[6:]
                match_data = ""
                for resp in response['attachments']:
                    match_level = 0
                    doc_private = doc_attachment.get('PRIVATE1', "")
                    if not doc_private:
                        doc_private = doc_attachment.get('PRIVATE', "")
                    if not doc_private:
                        doc_private = doc_attachment.get('ISPRIVATE', "")
                    if str(doc_attachment['SNO']).strip() == str("" if resp['attNo'] is None else resp['attNo']).strip():
                        match_level += 1
                    if str(doc_attachment['TITLE']).strip() == str("" if resp['title'] is None else resp['title']).strip():
                        #print "tit"
                        match_level += 1
                    if str(doc_attachment['ZTIME']).strip() == str("" if resp['zTime'] is None else resp['zTime']).strip():
                        #print "ztim"
                        match_level += 1
                    if str(doc_attachment['FILE_TYPE']).strip() == str("" if resp['fileType'] is None else resp['fileType']).strip():
                        #print "ft"
                        match_level += 1
                    if str(doc_private).strip() == str("" if resp['private1'] is None else resp['private1']).strip():
                        #print "pr1"
                        match_level += 1
                    if str(doc_attachment['DATE_CREATED']).strip() == str("" if resp['dateCreated'] is None else resp['dateCreated']).strip():
                        #print "dc"
                        match_level += 1
                    if str(doc_attachment['CREATED_BY']).strip() == str("" if resp['createdBy'] is None else resp['createdBy']).strip():
                        #print "cb"
                        match_level += 1
                    if str(doc_attachment['PATH']).strip() == str("" if resp['path'] is None else resp['path']).strip():
                        #print "pat"
                        match_level += 1
                    if str(doc_attachment['UPLOADED_BY']).strip() == str("" if resp['uploadedBy'] is None else resp['uploadedBy']).strip():
                        #print "uplob"
                        match_level += 1
                    if str(doc_attachment['SIZE1']).strip() == str("" if resp['size1'] is None else int(resp['size1'])).strip():
                        #print "s1"
                        match_level += 1
                    if str(zDate).strip() == str("" if resp['zDate'] is None else resp['zDate']).strip():
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
                    output += "\n************************************************"
                    output += "\nData Mismatch, max number of values matched is "+str(old_match_level)
                    output += "\nKafka ==> "+str(json.dumps(doc_attachment, sort_keys=True))
                    output += "\nAPI   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", "", "Incorrect value for 'attachment'!"]
                    table.append(tmp)
                    status = 1
                    output += "\n************************************************"
                else:
                    output += "\nData matched, highest level of match is "+str(match_level)
                    output += "\nKafka ==> "+str(json.dumps(doc_attachment, sort_keys=True))
                    output += "\nAPI   ==> "+str(json.dumps(match_data, sort_keys=True))
                    tmp = ["", "", "Match found for 'attachment'!"]
                    table.append(tmp)
            if status == 0:
                output += "\nMatch Found"
                row.append("Match Found")
            else:
                row.append("Data Mismatch")
                output += "\nCompared JSONs"
                output += "\nKafka: "+str(json.dumps(document['attachment'], sort_keys=True))
                output += "\nAPI: "+str(json.dumps(response['attachments'], sort_keys=True))
                output += "\n"
                output += tabulate(table, headers=["Kafka", "API", "Status"], tablefmt="rst")
        else:
            output += "\nNo Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        totalTime = datetime.datetime.now() - start_time
        row.append(totalTime)
        response_writer.writerow(row)
        return output


