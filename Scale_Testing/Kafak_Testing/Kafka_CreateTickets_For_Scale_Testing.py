import Queue
from threading import Thread
from threading import Thread
import csv
import traceback
import logging
import datetime
import time
import json
import Queue
from kafka import (KafkaClient, KeyedProducer, RoundRobinPartitioner)
from utils import ScaleUtils

su = ScaleUtils()
enclosure_queue = Queue.Queue()
sleep = 3
request_no = 0
f = open("Kafka_CreateTickets.csv", 'wt')
writer = csv.writer(f)
row = ["SNo", "KafakTime", "Topic", "Partition", "GivenPartition", "ErrorStatus", "Offset", "CaseID", \
       "APIStartTime", "APIEndTime", "APITotTime", "End2EndTime", "RetryCount",
       "APIStatus", "APIStatusCode", "APIElapsedTime", "APIResData", "DataStatus"]
writer.writerow(row)
rows = []

kafka = KafkaClient('172.22.147.232:9092,172.22.147.242:9092,172.22.147.243:9092')
#producer = KeyedProducer(kafka, async=False, req_acks=KeyedProducer.ACK_AFTER_LOCAL_WRITE, ack_timeout=2000, sync_fail_on_error=False)
producer = KeyedProducer(kafka, async=False, req_acks=KeyedProducer.ACK_AFTER_LOCAL_WRITE, ack_timeout=2000, sync_fail_on_error=False, partitioner=RoundRobinPartitioner)



def verify_ticket_details(case_data, row, kafka_time, sleep):
    hadoop_retry = 0
    hadoop_found = 0
    ofile = open('Kafka_automated_verify_ticket_details.txt', "a")
    output = ""
    try:
        et = ""
        st = datetime.datetime.now()
        time.sleep(sleep)
        while 1:
            response = ""
            response = su.request(case_data['srDetails']['caseId'])
            if response.status_code == 200:
                et = datetime.datetime.now()
                api_total_time = et - st
                end_2_end_time = et - kafka_time
                row.append(st)
                row.append(et)
                row.append(api_total_time)
                row.append(end_2_end_time)
                row.append(hadoop_retry)
                row.append("Found")
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text[:10])
                output += su.verify_ticket_details(case_data['srDetails'], response, output)
                row.append(su.result)
                hadoop_found = 1
                print "Found in Hadoop"
                break
            hadoop_retry += 1
            if hadoop_retry == 10:
                output += "Not found in Hadoop"
                et = datetime.datetime.now()
                api_total_time = et - st
                end_2_end_time = et - kafka_time
                row.append(st)
                row.append(et)
                row.append(api_total_time)
                row.append(end_2_end_time)
                row.append(hadoop_retry)
                row.append("Not Found")
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text)
                output += "Not found in Hadoop"
                row.append(su.result)
                break
            else:
                output += "Hadoop retries: "+str(hadoop_retry)
            time.sleep(1)
    except Exception:
        output += str(Exception.message)
        output += str(traceback.format_exc())
        print output
    output += "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    output += "\n\n"
    ofile.write(output)
    ofile.close()
    print "\n======================"
    print "Hadoop found: "+str(hadoop_found)
    print "======================"
    return row


def create_ticket(i, queue):
    while True:
        try:
            ticket_string = "JNPRNETWORKS"
            data = { "srDetails" : { "srCategory4" : "", "srCategory2" : "", "srCategory3" : "", "srCategory1" : "", "previousTeam" : "", "zzQ10" : "", "sirtBundle" : "", "endDate" : "00000000", "knowledgeArticle" : "", "outageInfoAvailable" : "", "sku" : "", "customerCaseNumber" : "", "yearRoundSupport" : "", "totalOutageTime" : "00000000", "zzQ3" : "", "zzQ2" : "", "zzQ1" : "", "outageImpactKey" : "", "zzQ7" : "", "zzQ6" : "", "zzQ5" : "", "zzQ4" : "", "ccList" : "", "zzQ9" : "", "zzQ8" : "", "processType" : "ZTEC", "ccEngineer" : "", "numberOfUsersAffected" : "", "contractId" : "", "criticalOutage" : "", "previousOwnerSkill" : "", "followupMethodKey" : "", "specialRelease" : "", "release" : "", "startDate" : "00000000", "warrantyEndDate" : "00000000", "escalation" : "", "jsaAdvisoryBoard" : "", "viaDescription" : "JSS request", "employeeId" : "", "severityKey" : "04", "secVulnerability" : "", "version" : "", "outageDescription" : "", "statusKey" : "E0004", "theaterDescription" : "AMER", "contractStatus" : "", "productSeries" : "M-Series", "escalationkey" : "0", "reason" : "Customer Responded", "outageCauseDescription" : "", "serviceProduct" : "", "processTypeDescription" : "Technical Service Request", "country" : "US", "courtesykey" : "", "betaType" : "", "entitlementChecked" : "", "internalUse" : "", "smeContact" : "", "software" : "", "reporterDetails" : "", "technicalCategory4" : "", "subReason" : "", "technicalCategory1" : "", "technicalCategory3" : "", "technicalCategory2" : "", "caseId" : "2015-1119-T-0038", "temperature" : "", "platform" : "M10i", "entitledSerialNumber" : "K1915", "priority" : "P4 - Low", "viaKey" : "ZJS", "srReqDate" : [ { "dateStamp" : "20151119173510", "duration" : "", "dateType" : "Last Update from Reporter", "timeUnit" : "" }, { "dateStamp" : "20151119173509", "duration" : "", "dateType" : "L1 Assignment Date", "timeUnit" : "" }, { "dateStamp" : "20151119173510", "duration" : "", "dateType" : "Ownership Date", "timeUnit" : "" }, { "dateStamp" : "20151119173509", "duration" : "", "dateType" : "First Responsible group assignment", "timeUnit" : "" }, { "dateStamp" : "20151119173506", "duration" : "", "dateType" : "Create Date", "timeUnit" : "" }, { "dateStamp" : "20151122173506", "duration" : "", "dateType" : "Requested Delivery Date Proposal", "timeUnit" : "" }, { "dateStamp" : "20151119173506", "duration" : "", "dateType" : "First Response By", "timeUnit" : "" }, { "dateStamp" : "20151119193508", "duration" : "", "dateType" : "ToDo By", "timeUnit" : "" }, { "dateStamp" : "20151119173509", "duration" : "", "dateType" : "JTAC L1 Assigned", "timeUnit" : "" }, { "dateStamp" : "20151119173510", "duration" : "", "dateType" : "Last Modified date", "timeUnit" : "" }, { "dateStamp" : "20151126173506", "duration" : "", "dateType" : "Update frequency", "timeUnit" : "" } ], "outageKey" : "", "priorityKey" : "04", "theaterKey" : "2", "escalationLevelDescription" : "", "outageTypeKey" : "", "entitlementServiceLevel" : "", "cve" : "", "urgencyKey" : "", "courtesyDescription" : "", "top5" : "", "criticalIssue" : "", "jtac" : "", "followupMethod" : "", "routerName" : "", "severity" : "S4 - Customer Problem/Query", "outsourcer" : "", "numberOfSystemsAffected" : "", "outageTypeDescription" : "", "build" : "", "cvss" : "", "productId" : "M10IBASE-AC", "status" : "Dispatch", "externallyReported" : "", "description" : "2015-11-19 23:05:02.476000 Some Issue Creating test case to verify SAP and Hadoop functionality", "raFa" : "", "entitlementSource" : "", "outageCauseKey" : "", "employeeEmail" : "", "partnerFunction" : [ { "partnerName" : "COMPUTER SCIENCE CORPORATION", "partnerId" : "0100167296", "partnerFunctionName" : "Sold-To Party", "partnerFunctionKey" : "00000001" }, { "partnerName" : "PSML1 JTAC-M-JTAC L1", "partnerId" : "0089512611", "partnerFunctionName" : "Responsible Group", "partnerFunctionKey" : "00000099" }, { "partnerName" : "Test Test", "partnerId" : "0200006266", "partnerFunctionName" : "Reporter (Person)", "partnerFunctionKey" : "00000151" } ], "escalationLevelKey" : "0", "overideOutage" : "", "serialNumber" : "K1915", "outageImpactDescription" : "", "urgency" : ""}}
            req = queue.get()
            case_data = data
            kk = req % 26
            #request_no = request_no + 1
            case_data['srDetails']['caseId'] = ticket_string+str(req)
            res = producer.send_messages(b'SAPEvent', b''+str(kk), bytes(json.dumps(case_data)))
            kafka_time = datetime.datetime.now()
            #print res
            res = res[0]
            topic = res[0]
            partition = res[1]
            error = res[2]
            offset = res[3]
            print "\nEventNo: "+str(req)+"\tThread "+str(i)+"\tKafkaKey: "+str(kk)+"\tProducerPartition: "+str(partition)+"\tCaseID: "+case_data['srDetails']['caseId']
            row = [req, kafka_time, topic, partition, kk, error, offset]
            kafka.close()
            if not error:
                case_key = case_data['srDetails']['caseId']
                row.append(case_key)
                #row = verify_ticket_details(case_data, row, kafka_time, sleep)
                rows.append(row)
                #writer.writerow(row)
        except Exception:
            error = traceback.format_exc()
            print error
        queue.task_done()

q = Queue.Queue()
# Set up some threads to fetch the enclosures

for i in range(500):
    worker = Thread(target=create_ticket, args=(i, q,))
    worker.setDaemon(True)
    worker.start()
start_time = datetime.datetime.now()
for i in range(10000):
    q.put(i)
# Now wait for the queue to be empty, indicating that we have
# processed all of the downloads.
print '*** Main thread waiting'
q.join()
print '*** Done'
print "Totaltime: "
print datetime.datetime.now() - start_time
for r in rows:
    writer.writerow(r)
f.close()