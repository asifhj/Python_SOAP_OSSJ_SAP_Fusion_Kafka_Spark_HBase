__author__ = 'asifj'
import requests
import time
from threading import Thread
import csv
import logging
import re
import traceback
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('automated_ticketing.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

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
            <v15:troubleDescription>201416-4- BNG-Basic_Netw_Funct - TestFR OSSJ Juniper 47 NE Name: BNG Component Name: BNG-Basic_Netw_Funct Card Type: - select or enter a value - Serial Number: GG0213130986 </v15:troubleDescription>
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
encoded_request = body.encode('utf-8')
headers = {'content-type': 'text/xml',
           "Content-Length": len(encoded_request)}

def create_ticket(i, url, body, encoded_request, headers, spamwriter):
    try:
        body = body.replace("<v15:troubleDescription>", "<v15:troubleDescription>"+str(i)+" ")
        row = [i]
        row.append(body.replace("\n",""))
        response = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Header xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"/><soap:Body><ns2:createTroubleTicketByValueResponse xmlns="http://ossj.org/xml/Common/v1-5" xmlns:ns10="http://ossj.org/xml/Common-SharedAlarm/v1-5" xmlns:ns11="http://docs.oasis-open.org/wsrf/bf-2" xmlns:ns12="http://www.w3.org/2005/08/addressing" xmlns:ns13="http://docs.oasis-open.org/wsn/b-2" xmlns:ns14="http://docs.oasis-open.org/wsn/t-1" xmlns:ns15="http://docs.oasis-open.org/wsrf/r-2" xmlns:ns2="http://ossj.org/xml/TroubleTicket/v1-2" xmlns:ns3="http://ossj.org/xml/Common-CBECore/v1-5" xmlns:ns4="http://ossj.org/xml/Common-CBEBi/v1-5" xmlns:ns5="http://ossj.org/xml/Common-CBELocation/v1-5" xmlns:ns6="http://ossj.org/xml/TroubleTicket-CBETrouble/v1-2" xmlns:ns7="http://ossj.org/xml/TroubleTicket_x790/v0-5" xmlns:ns8="http://ossj.org/xml/Common-CBEDatatypes/v1-5" xmlns:ns9="http://ossj.org/xml/Common-CBEParty/v1-5"><ns2:troubleTicketKey><type>org.ossj.xml.troubleticket_x790.v0_5.X790TroubleTicketKey</type><primaryKey><primaryKey>2015-1009-0173</primaryKey></primaryKey></ns2:troubleTicketKey></ns2:createTroubleTicketByValueResponse></soap:Body></soap:Envelope>'
        #response = requests.post(url, data=encoded_request, headers=headers, verify=False)
        #response = requests.request('GET', 'http://httpbin.org/get')
        #response.text = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Header xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"/><soap:Body><ns2:createTroubleTicketByValueResponse xmlns="http://ossj.org/xml/Common/v1-5" xmlns:ns10="http://ossj.org/xml/Common-SharedAlarm/v1-5" xmlns:ns11="http://docs.oasis-open.org/wsrf/bf-2" xmlns:ns12="http://www.w3.org/2005/08/addressing" xmlns:ns13="http://docs.oasis-open.org/wsn/b-2" xmlns:ns14="http://docs.oasis-open.org/wsn/t-1" xmlns:ns15="http://docs.oasis-open.org/wsrf/r-2" xmlns:ns2="http://ossj.org/xml/TroubleTicket/v1-2" xmlns:ns3="http://ossj.org/xml/Common-CBECore/v1-5" xmlns:ns4="http://ossj.org/xml/Common-CBEBi/v1-5" xmlns:ns5="http://ossj.org/xml/Common-CBELocation/v1-5" xmlns:ns6="http://ossj.org/xml/TroubleTicket-CBETrouble/v1-2" xmlns:ns7="http://ossj.org/xml/TroubleTicket_x790/v0-5" xmlns:ns8="http://ossj.org/xml/Common-CBEDatatypes/v1-5" xmlns:ns9="http://ossj.org/xml/Common-CBEParty/v1-5"><ns2:troubleTicketKey><type>org.ossj.xml.troubleticket_x790.v0_5.X790TroubleTicketKey</type><primaryKey><primaryKey>2015-1009-0173</primaryKey></primaryKey></ns2:troubleTicketKey></ns2:createTroubleTicketByValueResponse></soap:Body></soap:Envelope>'
        m = re.match(r'.+<primarykey>([\d|\-]+)</primaryKey>.+', response.text, re.I|re.M)
        if m:
            print m.groups(0)[0]
        else:
            print "Request No: "+str(i)+" did not processed\n"
            logger.error("Request No: "+str(i)+" did not processed\n"+str(response.text))
        row.append(response.text.replace("\n",""))
        logger.info("Request No: "+str(i)+" sent to OSSJ with SOAPEnvelope\n"+str(body))
        print "Request No: "+str(i)+" sent to OSSJ with SOAPEnvelope"
        logger.info("Request No: "+str(i)+" is processed with response\n"+str(response.text))
        print "Request No: "+str(i)+" is processed with response"
        spamwriter.writerow(row)
    except Exception:
        error = traceback.format_exc()
        print error
        logger.error("Request No: "+str(i)+" did not processed\n"+str(response.text))
        logger.error(error)

threads = []
with open('Tickets.csv', 'w') as csv_writer:
    spamwriter = csv.writer(csv_writer, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
    for i in range(1,2):
        with open('Tickets.csv', 'w') as csv_writer:
            thread = Thread(target=create_ticket, args=(i, url, body, encoded_request, headers, spamwriter))
            threads += [thread]
            thread.start()
    for thread in threads:
        thread.join()
