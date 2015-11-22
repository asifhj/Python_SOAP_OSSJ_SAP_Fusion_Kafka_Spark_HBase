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

    def get_account_details(self, document, row):
        #http://172.22.147.248:8092/api/user-manager/accounts
        print "API URL: "+self.url+"user-manager/accounts/"+str(document['header']['partnerId'])
        r = requests.get(self.url+"user-manager/accounts/"+str(document['header']['partnerId']))
        print "partnerId: "+str(document['header']['partnerId'])
        print "Response: "+str(r.status_code)
        keys = len(document.keys())
        print "Keys: "+str(keys)
        row.append(r.status_code)
        status = 0
        if r.status_code == 200:
            response = json.loads(r.text)
            table = []
            response_account_len = len(response)
            document_account_len = len(document)
            print "Number of account details in document: "+str(document_account_len)
            print "Number of account details in API response: "+str(response_account_len)
            header = document.get("header", "")
            market = document.get("marketingAttributes", "")
            address = document.get("address", "")
            relationship = document.get("relationship", "")
            if header:
                print "Verifying header attributes..."
                if not str(document["header"]["rating"]).strip() == ("" if response["rating"] is None else response["rating"]):
                    tmp = [str(document["header"]["rating"]).strip(), str(response["rating"]).strip(),
                           "Incorrect value for 'rating'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["tranBlockReason"]).strip() == ("" if response["tranBlockReason"] is None else response["tranBlockReason"]):
                    tmp = [str(document["header"]["tranBlockReason"]).strip(), str(response["tranBlockReason"]).strip(),
                           "Incorrect value for 'tranBlockReason'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountType"]).strip() == ("" if response["accountType"] is None else response["accountType"]):
                    tmp = [str(document["header"]["accountType"]).strip(), str(response["accountType"]).strip(),
                           "Incorrect value for 'accountType'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["customerSince"]).strip() == ("" if response["customerSince"] is None else response["customerSince"]):
                    tmp = [str(document["header"]["customerSince"]).strip(), str(response["customerSince"]).strip(),
                           "Incorrect value for 'customerSince'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["ratingKey"]).strip() == ("" if response["ratingKey"] is None else response["ratingKey"]):
                    tmp = [str(document["header"][""]).strip(), str(response["ratingKey"]).strip(),
                           "Incorrect value for 'ratingKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["sapChangeTime"]).strip() == ("" if response["changeTime"] is None else response["changeTime"]):
                    tmp = [str(document["header"]["sapChangeTime"]).strip(), str(response["changeTime"]).strip(),
                           "Incorrect value for 'sapChangeTime'!"]
                    table.append(tmp)
                    status = 1
                '''if not str(document["header"][""]).strip() == ("" if response[""] is None else response["sapCreateDate"]):
                    tmp = [str(document["header"][""]).strip(), str(response[""]).strip(),
                           "Incorrect value for 'ccEngineer'!"]
                    table.append(tmp)
                    status = 1'''
                if not str(document["header"]["commonId"]).strip() == ("" if response["commonId"] is None else response["commonId"]):
                    tmp = [str(document["header"]["commonId"]).strip(), str(response["commonId"]).strip(),
                           "Incorrect value for 'commonId'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["sapCreateTime"]).strip() == ("" if response["createTime"] is None else response["createTime"]):
                    tmp = [str(document["header"][""]).strip(), str(response["createTime"]).strip(),
                           "Incorrect value for 'sapCreateTime'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountTypeKey"]).strip() == ("" if response["accountTypeKey"] is None else response["accountTypeKey"]):
                    tmp = [str(document["header"]["accountTypeKey"]).strip(), str(response["accountTypeKey"]).strip(),
                           "Incorrect value for 'accountTypeKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["archivingFlag"]).strip() == ("" if response["archivingFlag"] is None else response["archivingFlag"]):
                    tmp = [str(document["header"]["archivingFlag"]).strip(), str(response["archivingFlag"]).strip(),
                           "Incorrect value for 'archivingFlag'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["partnerGrouping"]).strip() == ("" if response["partnerGrouping"] is None else response["partnerGrouping"]):
                    tmp = [str(document["header"]["partnerGrouping"]).strip(), str(response["partnerGrouping"]).strip(),
                           "Incorrect value for 'partnerGrouping'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountName"]).strip() == ("" if response["accountName"] is None else response["accountName"]):
                    tmp = [str(document["header"]["accountName"]).strip(), str(response["accountName"]).strip(),
                           "Incorrect value for 'accountName'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountClassKey"]).strip() == ("" if response["accountClassKey"] is None else response["accountClassKey"]):
                    tmp = [str(document["header"]["accountClassKey"]).strip(), str(response["accountClassKey"]).strip(),
                           "Incorrect value for 'accountClassKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["statusKey"]).strip() == ("" if response["statusKey"] is None else response["statusKey"]):
                    tmp = [str(document["header"]["statusKey"]).strip(), str(response["statusKey"]).strip(),
                           "Incorrect value for 'statusKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["partnerGroupingKey"]).strip() == ("" if response["partnerGroupingKey"] is None else response["partnerGroupingKey"]):
                    tmp = [str(document["header"]["partnerGroupingKey"]).strip(), str(response["partnerGroupingKey"]).strip(),
                           "Incorrect value for 'partnerGroupingKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["serviceRenewalDate"]).strip() == ("" if response["serviceRenewalDate"] is None else response["serviceRenewalDate"]):
                    tmp = [str(document["header"]["serviceRenewalDate"]).strip(), str(response["serviceRenewalDate"]).strip(),
                           "Incorrect value for 'serviceRenewalDate'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["status"]).strip() == ("" if response["status"] is None else response["status"]):
                    tmp = [str(document["header"]["status"]).strip(), str(response["status"]).strip(),
                           "Incorrect value for 'status'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["tranBlockReasonKey"]).strip() == ("" if response["tranBlockReasonKey"] is None else response["tranBlockReasonKey"]):
                    tmp = [str(document["header"]["tranBlockReasonKey"]).strip(), str(response["tranBlockReasonKey"]).strip(),
                           "Incorrect value for 'tranBlockReasonKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountGroup"]).strip() == ("" if response["accountGroup"] is None else response["accountGroup"]):
                    tmp = [str(document["header"]["accountGroup"]).strip(), str(response["accountGroup"]).strip(),
                           "Incorrect value for 'accountGroup'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["sapCreateDate"]).strip() == ("" if response["createDate"] is None else response["createDate"]):
                    tmp = [str(document["header"]["sapCreateDate"]).strip(), str(response["createDate"]).strip(),
                           "Incorrect value for 'sapCreateDate'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["dataOriginKey"]).strip() == ("" if response["dataOriginKey"] is None else response["dataOriginKey"]):
                    tmp = [str(document["header"]["dataOriginKey"]).strip(), str(response["dataOriginKey"]).strip(),
                           "Incorrect value for 'dataOriginKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountClass"]).strip() == ("" if response["accountClass"] is None else response["accountClass"]):
                    tmp = [str(document["header"]["accountClass"]).strip(), str(response["accountClass"]).strip(),
                           "Incorrect value for 'accountClass'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["sapChangeDate"]).strip() == ("" if response["changeDate"] is None else response["changeDate"]):
                    tmp = [str(document["header"]["sapChangeDate"]).strip(), str(response["changeDate"]).strip(),
                           "Incorrect value for 'sapChangeDate/changeDate'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["contactFirstName"]).strip() == ("" if response["firstName"] is None else response["firstName"]):
                    tmp = [str(document["header"]["contactFirstName"]).strip(), str(response["firstName"]).strip(),
                           "Incorrect value for 'contactFirstName'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["accountGroupKey"]).strip() == ("" if response["accountGroupKey"] is None else response["accountGroupKey"]):
                    tmp = [str(document["header"]["accountGroupKey"]).strip(), str(response["accountGroupKey"]).strip(),
                           "Incorrect value for 'accountGroupKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["dataOrigin"]).strip() == ("" if response["dataOrigin"] is None else response["dataOrigin"]):
                    tmp = [str(document["header"]["dataOrigin"]).strip(), str(response["dataOrigin"]).strip(),
                           "Incorrect value for 'dataOrigin'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["partnerType"]).strip() == ("" if response["partnerType"] is None else response["partnerType"]):
                    tmp = [str(document["header"]["partnerType"]).strip(), str(response["partnerType"]).strip(),
                           "Incorrect value for 'partnerType'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["contactLastNname"]).strip() == ("" if response["lastName"] is None else response["lastName"]):
                    tmp = [str(document["header"]["contactLastNname"]).strip(), str(response["lastName"]).strip(),
                           "Incorrect value for 'contactLastNname'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["header"]["partnerTypeKey"]).strip() == ("" if response["partnerKeyType"] is None else response["partnerKeyType"]):
                    tmp = [str(document["header"]["partnerTypeKey"]).strip(), str(response["partnerKeyType"]).strip(),
                           "Incorrect value for 'partnerTypeKey'!"]
                    table.append(tmp)
                    status = 1
            else:
                print "No header attributes found in document!"
            if address:
                print "Verifying address attributes..."
                if not str(document["address"]["website"]).strip() == ("" if response["website"] is None else response["website"]):
                    tmp = [str(document["address"]["website"]).strip(), str(response["website"]).strip(),
                           "Incorrect value for 'website'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["city"]).strip() == ("" if response["city"] is None else response["city"]):
                    tmp = [str(document["address"]["city"]).strip(), str(response["city"]).strip(),
                           "Incorrect value for 'city'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["language"]).strip() == ("" if response["language"] is None else response["language"]):
                    tmp = [str(document["address"]["language"]).strip(), str(response["language"]).strip(),
                           "Incorrect value for 'language'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["extension"]).strip() == ("" if response["extension"] is None else response["extension"]):
                    tmp = [str(document["address"]["extension"]).strip(), str(response["extension"]).strip(),
                           "Incorrect value for 'extension'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["street1"]).strip() == ("" if response["street1"] is None else response["street1"]):
                    tmp = [str(document["address"]["street1"]).strip(), str(response["street1"]).strip(),
                           "Incorrect value for 'street1'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["street2"]).strip() == ("" if response["street2"] is None else response["street2"]):
                    tmp = [str(document["address"]["street2"]).strip(), str(response["street2"]).strip(),
                           "Incorrect value for 'street2'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["street3"]).strip() == ("" if response["street3"] is None else response["street3"]):
                    tmp = [str(document["address"]["street3"]).strip(), str(response["street3"]).strip(),
                           "Incorrect value for 'street3'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["street4"]).strip() == ("" if response["street4"] is None else response["street4"]):
                    tmp = [str(document["address"]["street4"]).strip(), str(response["street4"]).strip(),
                           "Incorrect value for 'street4'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["communicationType"]).strip() == ("" if response["communicationType"] is None else response["communicationType"]):
                    tmp = [str(document["address"]["communicationType"]).strip(), str(response["communicationType"]).strip(),
                           "Incorrect value for 'communicationType'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["phone"]).strip() == ("" if response["phone"] is None else response["phone"]):
                    tmp = [str(document["address"]["phone"]).strip(), str(response["phone"]).strip(),
                           "Incorrect value for 'phone'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["email"]).strip() == ("" if response["email"] is None else response["email"]):
                    tmp = [str(document["address"]["email"]).strip(), str(response["email"]).strip(),
                           "Incorrect value for 'email'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["communicationTypeKey"]).strip() == ("" if response["communicationTypeKey"] is None else response["communicationTypeKey"]):
                    tmp = [str(document["address"]["communicationTypeKey"]).strip(), str(response["communicationTypeKey"]).strip(),
                           "Incorrect value for 'communicationTypeKey'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["country"]).strip() == ("" if response["country"] is None else response["country"]):
                    tmp = [str(document["address"]["country"]).strip(), str(response["country"]).strip(),
                           "Incorrect value for 'country'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["postalCode"]).strip() == ("" if response["postalCode"] is None else response["postalCode"]):
                    tmp = [str(document["address"]["postalCode"]).strip(), str(response["postalCode"]).strip(),
                           "Incorrect value for 'postalCode'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["houseNumber"]).strip() == ("" if response["houseNumber"] is None else response["houseNumber"]):
                    tmp = [str(document["address"]["houseNumber"]).strip(), str(response["houseNumber"]).strip(),
                           "Incorrect value for 'houseNumber'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["region"]).strip() == ("" if response["region"] is None else response["region"]):
                    tmp = [str(document["address"]["region"]).strip(), str(response["region"]).strip(),
                           "Incorrect value for 'region'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["address"]["transportZone"]).strip() == ("" if response["transportZone"] is None else response["transportZone"]):
                    tmp = [str(document["address"]["transportZone"]).strip(), str(response["transportZone"]).strip(),
                           "Incorrect value for 'transportZone'!"]
                    table.append(tmp)
                    status = 1
            else:
                print "No address attributes found in d,ocument!"

            if market:
                print "Verifying address attributes..."
                if not str(document["marketingAttributes"]["serviceRequestEntitlement"]).strip() == ("" if response["serviceRequestEntitlement"] is None else response["serviceRequestEntitlement"]):
                    tmp = [str(document["marketingAttributes"]["serviceRequestEntitlement"]).strip(), str(response["serviceRequestEntitlement"]).strip(),
                           "Incorrect value for 'serviceRequestEntitlement'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["srByEmail"]).strip() == ("" if response["srByEmail"] is None else response["srByEmail"]):
                    tmp = [str(document["marketingAttributes"]["srByEmail"]).strip(), str(response["srByEmail"]).strip(),
                           "Incorrect value for 'srByEmail'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["citizenship"]).strip() == ("" if response["citizenship"] is None else response["citizenship"]):
                    tmp = [str(document["marketingAttributes"]["citizenship"]).strip(), str(response["citizenship"]).strip(),
                           "Incorrect value for 'citizenship'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["rmaEntitlement"]).strip() == ("" if response["rmaEntitlement"] is None else response["rmaEntitlement"]):
                    tmp = [str(document["marketingAttributes"]["rmaEntitlement"]).strip(), str(response["rmaEntitlement"]).strip(),
                           "Incorrect value for 'rmaEntitlement'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["srEntitlement"]).strip() == ("" if response["srEntitlement"] is None else response["srEntitlement"]):
                    tmp = [str(document["marketingAttributes"]["srEntitlement"]).strip(), str(response["srEntitlement"]).strip(),
                           "Incorrect value for 'srEntitlement'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["authorizedForRma"]).strip() == ("" if response["autorizedForRMA"] is None else response["autorizedForRMA"]):
                    tmp = [str(document["marketingAttributes"]["authorizedForRma"]).strip(), str(response["autorizedForRMA"]).strip(),
                           "Incorrect value for 'authorizedForRma/autorizedForRMA'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["accountTemperature"]).strip() == ("" if response["accountTemperature"] is None else response["accountTemperature"]):
                    tmp = [str(document["marketingAttributes"]["accountTemperature"]).strip(), str(response["accountTemperature"]).strip(),
                           "Incorrect value for 'accountTemperature'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["entitlementValidTill"]).strip() == ("" if response["entitlementValidTill"] is None else response["entitlementValidTill"]):
                    tmp = [str(document["marketingAttributes"]["entitlementValidTill"]).strip(), str(response["entitlementValidTill"]).strip(),
                           "Incorrect value for 'entitlementValidTill'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["temperatureEndDate"]).strip() == ("" if response["temperatureEndDate"] is None else response["temperatureEndDate"]):
                    tmp = [str(document["marketingAttributes"]["temperatureEndDate"]).strip(), str(response["temperatureEndDate"]).strip(),
                           "Incorrect value for 'temperatureEndDate'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["accountServiceLevel"]).strip() == ("" if response["accountServiceLevel"] is None else response["accountServiceLevel"]):
                    tmp = [str(document["marketingAttributes"]["accountServiceLevel"]).strip(), str(response["accountServiceLevel"]).strip(),
                           "Incorrect value for 'accountServiceLevel'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["analysisFlag"]).strip() == ("" if response["analysisFlag"] is None else response["analysisFlag"]):
                    tmp = [str(document["marketingAttributes"]["analysisFlag"]).strip(), str(response["analysisFlag"]).strip(),
                           "Incorrect value for 'analysisFlag'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["courtesyCall"]).strip() == ("" if response["courtesyCall"] is None else response["courtesyCall"]):
                    tmp = [str(document["marketingAttributes"]["courtesyCall"]).strip(), str(response["courtesyCall"]).strip(),
                           "Incorrect value for 'courtesyCall'!"]
                    table.append(tmp)
                    status = 1
                '''
                if not str(document["marketingAttributes"]["abcd"]).strip() == ("" if response["abcd"] is None else response["abcd"]):
                    tmp = [str(document["marketingAttributes"]["abcd"]).strip(), str(response["abcd"]).strip(),
                           "Incorrect value for 'ccEngineer'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["abcd"]).strip() == ("" if response["abcd"] is None else response["abcd"]):
                    tmp = [str(document["marketingAttributes"]["abcd"]).strip(), str(response["abcd"]).strip(),
                           "Incorrect value for 'ccEngineer'!"]
                    table.append(tmp)
                    status = 1
                if not str(document["marketingAttributes"]["abcd"]).strip() == ("" if response["abcd"] is None else response["abcd"]):
                    tmp = [str(document["marketingAttributes"]["abcd"]).strip(), str(response["abcd"]).strip(),
                           "Incorrect value for 'ccEngineer'!"]
                    table.append(tmp)
                    status = 1'''
            else:
                print "No address attributes found in document!"

                tmp = []
            if status == 0:
                print "Match Found"
                row.append("Match Found")
            else:
                print tabulate(table, headers=["LinkNo", "Key", "Kafka", "API", "Status"], tablefmt="rst")

        else:
            print "No Match Found in Hadoop."
            row.append("No Match Found in Hadoop.")
        return row

#client = MongoClient('10.219.48.134', 27017)
client = MongoClient('192.168.56.101', 27017)
db = client['SAPEvent']
collection = db['customerMaster']
api = HBase()
document_no = 0
documents = collection.find({})
#documents = collection.find({'header.partnerId': 100001407})
ofile = open('customerMaster.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
row = ["SNo", "CaseID", "KafkaJSON", "APIResponse", "Status"]
writer.writerow(row)
for document in documents:
    row = []
    document_no += 1
    row.append(document_no)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "Document No: "+str(document_no)
    if not document.get("header", ""):
        print "No header in document..."
        row.append("NA")
        row.append(str(document).replace("\n", ""))
        row.append("No header in document")
    else:
        row.append(document['header']['partnerId'])
        row.append(str(document).replace("\n", ""))
        try:
            row = api.get_account_details(document, row)
        except Exception:
            print Exception.message
            print(traceback.format_exc())
    writer.writerow(row)
    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print "\n\n"
ofile.close()

