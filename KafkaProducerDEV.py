__author__ = 'asifj'
from kafka import SimpleProducer, KafkaClient
import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)
data = {"srDetails2": {
    "knowledgeArticle": "",
    "reason": "New",
    "zzQ10": "",
    "priorityKey": 2,
    "severityKey": "",
    "endDate": "00000000",
    "temperature": "",
    "statusKey": "E0001",
    "outsourcer": "",
    "productSeries": "",
    "smeContact": "",
    "criticalOutage": "",
    "platform": "",
    "viaKey": 202,
    "srCategory4": "",
    "srCategory3": "Uplift Quote (RMA or CE)",
    "srCategory2": "Other",
    "courtesykey": "",
    "srCategory1": "Logistics",
    "yearRoundSupport": "",
    "entitlementServiceLevel": "",
    "outageDescription": "",
    "caseId": "2015-1005-T-0010",
    "courtesyDescription": "",
    "numberOfUsersAffected": "",
    "warrantyEndDate": "00000000",
    "partnerFunction": [
        {
            "partnerName": "JUNIPER NETWORKS",
            "partnerFunctionName": "Sold-To Party",
            "partnerFunctionKey": "00000001",
            "partnerId": "0010001253"
        },
        {
            "partnerName": "Goverthanan S",
            "partnerFunctionName": "Employee Responsible",
            "partnerFunctionKey": "00000014",
            "partnerId": "0000014748"
        },
        {
            "partnerName": "PSACXL1 JTAC-ACX-JTAC L1",
            "partnerFunctionName": "Responsible Group",
            "partnerFunctionKey": "00000099",
            "partnerId": "0089514462"
        },
        {
            "partnerName": "Goverthanan S",
            "partnerFunctionName": "Reporter (Person)",
            "partnerFunctionKey": "00000151",
            "partnerId": "0000014748"
        },
        {
            "partnerName": "Goverthanan S",
            "partnerFunctionName": "Created By",
            "partnerFunctionKey": "ZCRBY",
            "partnerId": "0000014748"
        },
        {
            "partnerName": "Goverthanan S",
            "partnerFunctionName": "Modified By",
            "partnerFunctionKey": "ZMODBY",
            "partnerId": "0000014748"
        }
    ],
    "routerName": "",
    "startDate": "00000000",
    "criticalIssue": "",
    "specialRelease": "",
    "entitlementSource": "",
    "outageImpactKey": "",
    "build": "",
    "numberOfSystemsAffected": "",
    "version": "",
    "outageInfoAvailable": "",
    "entitlementChecked": "",
    "jsaAdvisoryBoard": "",
    "escalationLevelDescription": "",
    "reporterDetails": "",
    "cve": "",
    "ouatgeCauseDescription": "",
    "outageTypeDescription": "",
    "technicalCategory1": "",
    "technicalCategory2": "",
    "secVulnerability": "",
    "ccList": "",
    "theaterKey": 2,
    "theaterDescription": "AMER",
    "srReqDate": [
        {
            "dateStamp": 20151005105523,
            "duration": "",
            "timeUnit": "",
            "dateType": "First Responsible group assignment"
        },
        {
            "dateStamp": 20151005105523,
            "duration": "",
            "timeUnit": "",
            "dateType": "Ownership Date"
        },
        {
            "dateStamp": 20151005105828,
            "duration": "",
            "timeUnit": "",
            "dateType": "Last Modified by SAP User"
        },
        {
            "dateStamp": 20151005115523,
            "duration": "",
            "timeUnit": "",
            "dateType": "First Response By"
        },
        {
            "dateStamp": 20151008105452,
            "duration": "",
            "timeUnit": "",
            "dateType": "Requested Delivery Date Proposal"
        },
        {
            "dateStamp": 20151005105452,
            "duration": "",
            "timeUnit": "",
            "dateType": "Create Date"
        }
    ],
    "technicalCategory3": "",
    "technicalCategory4": "",
    "employeeId": "GOVI",
    "previousOwnerSkill": "",
    "serialNumber": "",
    "entitledSerialNumber": "",
    "sirtBundle": "",
    "software": "",
    "priority": "P2 - High",
    "description": "test1 prob desc akdalkjdlksajdslkajdlka",
    "contractId": "",
    "followupMethodKey": "ESEC",
    "top5": "",
    "ccEngineer": "",
    "status": "Submitted",
    "escalationkey": 0,
    "sku": "",
    "contractStatus": "",
    "country": "US",
    "processType": "ZADM",
    "serviceProduct": "",
    "betaType": "",
    "totalOutageTime": "00000000",
    "release": "",
    "externallyReported": "",
    "notesFileName": "2015-1005-T-0010_20151005_170157.txt",
    "viaDescription": "Telephone call",
    "processTypeDescription": "Admin Service Request",
    "outageImpactDescription": "",
    "raFa": "",
    "followupMethod": "Email Secure Web Link",
    "previousTeam": "",
    "employeeEmail": "PMAURICIO##@XXJUNIPER.NETX",
    "cvss": "",
    "zzQ9": "",
    "zzQ8": "",
    "zzQ7": "",
    "severity": "",
    "overideOutage": "",
    "zzQ6": "",
    "zzQ5": "",
    "zzQ4": "",
    "zzQ3": "",
    "zzQ2": "",
    "productId": "",
    "zzQ1": "",
    "outageKey": "",
    "jtac": "",
    "outageCauseKey": "",
    "escalation": "",
    "customerCaseNumber": "",
    "outageTypeKey": ""
}}
# To send messages synchronously
kafka = KafkaClient('172.22.147.232:9092,172.22.147.242:9092,172.22.147.243:9092')
producer = SimpleProducer(kafka)

# Note that the application is responsible for encoding messages to type bytes
producer.send_messages(b'SAPEvent', b''+str(data)+'')
producer.send_messages(b'SAPEvent')

# Send unicode message
#producer.send_messages(b'SAPEvent', u'?????'.encode('utf-8'))