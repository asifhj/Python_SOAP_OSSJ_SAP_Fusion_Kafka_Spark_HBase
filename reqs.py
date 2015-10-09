__author__ = 'asifj'
import requests
import time
from threading import Thread
import csv
import logging

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
    #print "thread %d sleeps for 5 seconds" % i
    #time.sleep(5)
    #print "thread %d woke up" % i
    try:
        body = body.replace("<v15:troubleDescription>", "<v15:troubleDescription>"+str(i)+" ")
        row = []
        row.append(i+1)
        row.append(body.replace("\n",""))
        response = "hi"
        row.append(response.replace("\n",""))
        logger.info("Request No: "+str(i+1)+" sent to OSSJ with SOAPEnvelope\n"+str(body))
        #response = requests.post(url, data=encoded_request, headers=headers, verify=False)
        print "Request No: "+str(i+1)+" sent to OSSJ with SOAPEnvelope"
        #print(response.text)
        spamwriter.writerow(row)
    except:
        logger.error("Request No: "+str(i+1)+" did not processed\n"+str(response.text))

threads = []
with open('Tickets.csv', 'w') as csv_writer:
    spamwriter = csv.writer(csv_writer, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
    for i in range(5):
        with open('Tickets.csv', 'w') as csv_writer:
            thread = Thread(target=create_ticket, args=(i, url, body, encoded_request, headers, spamwriter))
            threads += [thread]
            thread.start()
    for thread in threads:
        thread.join()
