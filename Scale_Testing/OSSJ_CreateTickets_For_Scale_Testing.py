import requests
from threading import Thread
import csv
import re
import traceback
import logging
import datetime
from pymongo import MongoClient
import time
from utils import ScaleUtils
import uuid

__author__ = 'asifj'
# Create tickets through OSS/J
# create logger
logger = logging.getLogger('OSS/J-SOAP-SAP')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.FileHandler('automated_ticketing.log')
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

username = 'super'
password = 'juniper123'
case_id = '2015-0420-0052'
body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://ossj.org/xml/TroubleTicket/v1-2" xmlns:v11="http://ossj.org/xml/Common/v1-5" xmlns:v12="http://ossj.org/xml/Common-CBECore/v1-5" xmlns:v13="http://ossj.org/xml/Common-CBEBi/v1-5" xmlns:v14="http://ossj.org/xml/Common-CBELocation/v1-5" xmlns:v15="http://ossj.org/xml/TroubleTicket-CBETrouble/v1-2" xmlns:v16="http://ossj.org/xml/Common-CBEParty/v1-5" xmlns:v17="http://ossj.org/xml/Common-CBEDatatypes/v1-5" xmlns:v0="http://ossj.org/xml/TroubleTicket_x790/v0-5" xmlns:v18="http://ossj.org/xml/Common-SharedAlarm/v1-5">
   <soapenv:Header>
      <wsse:Security soapenv:mustUnderstand="0" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <wsse:UsernameToken wsse:Id="UsernameToken-14327075" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <wsse:Username>%s</wsse:Username>
      <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">%s</wsse:Password>
      </wsse:UsernameToken>
      </wsse:Security>
   </soapenv:Header>
   <soapenv:Body>
      <v1:createTroubleTicketByValueRequest>
         <v1:troubleTicketValue>
           <v15:troubleDetectionTime>2015-11-09T00:00:00.000+05:00</v15:troubleDetectionTime>
            <v15:troubleDescription>Creating test case to verify SAP and Hadoop functionality</v15:troubleDescription>
            <v0:suspectObjectList>
            <v0:item>
                  <v0:suspectObjectId>K1915</v0:suspectObjectId>
               </v0:item>
            </v0:suspectObjectList>
           </v1:troubleTicketValue>
      </v1:createTroubleTicketByValueRequest>
   </soapenv:Body>
</soapenv:Envelope>
""" % (username, password)
url="https://10.204.95.205/aimOSSTroubleTicketService/JVTTroubleTicketWS"
url="https://10.219.30.73/aimOSSTroubleTicketService/JVTTroubleTicketWS"


def verify_ticket_details(caseId, row, response_time, sleep):
    client = MongoClient('10.219.48.134', 27017)
    db = client['SAPEvent']
    collection = db['srDetails']
    mongodb_retry = 0
    hadoop_retry = 0
    mongodb_found = 0
    hadoop_found = 0
    document = ""
    key = {'caseId': caseId}
    ofile = open('verify_ticket_details.txt', "a")
    output = ""
    try:
        utils = ScaleUtils()
        response = ""
        et = ""
        st = datetime.datetime.now()
        time.sleep(sleep)
        while 1:
            response = ""
            response = utils.request(caseId)
            if response.status_code == 200:
                hadoop_found = 1
                print "Found in Hadoop"
                et = datetime.datetime.now()
                while 1:
                    document = collection.find_one(key)
                    print "Key: "+str(key)
                    if document:
                        row.append(mongodb_retry)
                        row.append("Found")
                        print "Found in MongoDB/Kafka"
                        mongodb_found = 1
                        break
                    mongodb_retry += 1
                    if mongodb_retry == 5:
                        print "Not found in MongoDB/Kafka"
                        row.append(mongodb_retry)
                        row.append("Not Found")
                        break
                    else:
                        print "MongoDB retries: "+str(mongodb_retry)
                    time.sleep(1)
                row.append(st)
                row.append(et)
                row.append(hadoop_retry)
                row.append("Found")
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text[:10])
                end_time = datetime.datetime.now()
                row.append(end_time)
                total_time = end_time-response_time
                row.append(str(total_time))
                output += utils.verify_ticket_details(document, response, output)
                row.append(utils.result)
                break
            hadoop_retry += 1
            if hadoop_retry == 5:
                output += "Not found in Hadoop"
                et = datetime.datetime.now()
                while 1:
                    document = collection.find_one(key)
                    print "Key: "+str(key)
                    if document:
                        row.append(mongodb_retry)
                        row.append("Found")
                        print "Found in MongoDB/Kafka"
                        mongodb_found = 1
                        break
                    mongodb_retry += 1
                    if mongodb_retry == 5:
                        print "Not found in MongoDB/Kafka"
                        row.append(mongodb_retry)
                        row.append("Not Found")
                        break
                    else:
                        print "MongoDB retries: "+str(mongodb_retry)
                    time.sleep(1)
                #response = utils.request(caseId)
                row.append(st)
                row.append(et)
                row.append(hadoop_retry)
                row.append("Not Found")
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text[:10])
                end_time = datetime.datetime.now()
                row.append(end_time)
                total_time = end_time-response_time
                row.append(str(total_time))
                row.append(utils.result)
                break
            else:
                output += "Hadoop retries: "+str(hadoop_retry)
            time.sleep(1)
    except Exception:
        output += str(Exception.message)
        output += str(traceback.format_exc())
    output += "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    output += "\n\n"
    ofile.write(output)
    ofile.close()
    print "======================"
    print "MongoDB/Kafka found: "+str(mongodb_found)
    print "Hadoop found: "+str(hadoop_found)
    print "======================"
    return row


def create_ticket(i, url, body, spamwriter, logger, sleep):
    try:
        body = body.replace("<v15:troubleDescription>", "<v15:troubleDescription>"+str(datetime.datetime.now())+" Some Issue ")
        encoded_request = body.encode('utf-8')
        headers = {'content-type': 'text/xml', "Content-Length": len(encoded_request)}
        response = ""
        row = [i, datetime.datetime.now()]
        response = requests.post(url, data=encoded_request, headers=headers, verify=False)
        response_time = datetime.datetime.now()
        row.append(response_time)
        m = re.match(r'.+<primarykey>([\d|\-]+)</primaryKey>.+', response.text, re.I | re.M)
        if m and response.status_code == 200:
            row.append("ADDED")
            case_key = m.groups(0)[0]
            case_key = str(case_key[:10])+"T-"+str(case_key[10:])
            row.append(case_key)
            row.append(response.status_code)
            row.append(response.elapsed)
            row.append(response.text.replace("\n",""))
            row = verify_ticket_details(case_key, row, response_time, sleep)
            logger.info("Request No: "+str(i)+" sent to OSSJ with SOAPEnvelope\nRequest No: "+str(i)+" is processed with response\n"+str(response.text)+"CaseID: "+str(m.groups(0)[0])+"\n")
            print "Request No: "+str(i)+" sent to OSSJ with SOAPEnvelope \nRequest No: "+str(i)+" is processed with response."
            print response.text
            print "\nCaseID: "+str(case_key+"\n")
        else:
            m = re.match(r'.+combination. Trouble Ticket Id: ([\d|\-]+).+', response.text, re.I | re.M)
            if m:
                row.append("EXISTS")
                row.append(m.groups(0)[0])
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text.replace("\n",""))
                print "Request No: "+str(i)+" did not processed\n"
                logger.error("Request No: "+str(i)+" did not processed, because its exists\n"+str(response.text)+"\n")
            else:
                row.append("Server/Request Error")
                row.append("")
                row.append(response.status_code)
                row.append(response.elapsed)
                row.append(response.text.replace("\n",""))
                print "Request No: "+str(i)+" did not processed\n"
                logger.error("Request No: "+str(i)+" did not processed, server/request problem.\n"+str(response.text)+"\n")
        spamwriter.writerow(row)
    except Exception:
        error = traceback.format_exc()
        print error
        logger.error("Request No: "+str(i)+" did not processed\n"+str(response.text)+"\n"+error+"\n")
        '''logger.info('info message')
        logger.warn('warn message')
        logger.error('error message')
        logger.critical('critical message')'''

threads = []
with open('10-10-Tickets-at-1-shot-with-sleep-10.csv', 'w') as csv_writer:
    spamwriter = csv.writer(csv_writer, delimiter=',', quotechar='\'')
    row = ["SNo", "CTRequestTime", "CTResponseTime", "CTStatus", "CaseID", "CTResponseStatus", "CTElapsedTime", "CTResponseData", \
           "MongoDBRetries", "MongoDBStatus", \
           "HadoopRequestTime", "HadoopResponseTime", "HadoopRetires", "HadoopStatus", "HadoopResponseStatus", \
           "HadoopElapsedTime", "HadoopResponseData", "EndTime", "EndToEndProcessTime", "DataStatus"]
    spamwriter.writerow(row)
    sleep = 10
    tickets = 2
    for i in range(1, tickets):
        print "Request "+str(i)
        thread = Thread(target=create_ticket, args=(i, url, body, spamwriter, logger, sleep))
        threads += [thread]
        thread.start()
    for thread in threads:
        thread.join()
    for i in range(1, tickets):
        print "Request "+str(i)
        thread = Thread(target=create_ticket, args=(i, url, body, spamwriter, logger, sleep))
        threads += [thread]
        thread.start()
    for thread in threads:
        thread.join()