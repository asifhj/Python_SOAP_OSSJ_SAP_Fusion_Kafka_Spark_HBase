import csv
import traceback
import logging
from pymongo import MongoClient
import datetime
from Queue import Queue
from utilsDataFile import Utils
import json
from threading import Thread
from colorama import init
init(autoreset=True)
__author__ = 'asifj'


logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)


SR = 0
KBLINKS = 0
ATTACHMENTS = 1
DOCUMENT_NO = 0
DOCUMENT_NO_END = 0


def worker_func_sr(utils, queue, output_writer, response_writer):
    while True:
        data = queue.get()
        document = data[0]
        document_no = data[1]
        startTime = data[2]
        output = ""
        try:
            output += utils.header(document, document_no, "SRID")
            response = utils.request(document, "SRID")
            output += "\n*******************************************"
            output += "\nVerifying srDetails..."
            output += "\n*******************************************"
            output += utils.validate_sr_details(response, document, document_no, startTime, response_writer)
            client = MongoClient('10.219.48.134', 27017)
            db = client['ImportedEvents']
            collection_new = db['srKbLink-new']
            key = {'caseId': document['SRID']}
            document_kbLinks = collection_new.find_one({'caseId': document['SRID']})
            res = json.loads(response.text)
            output += "\n\n\n*******************************************"
            output += "\nVerifying kbLinks..."
            output += "\n*******************************************"
            if document_kbLinks is None:
                output += "\n No kblinks found in document"
                if not res['kbLinks']:
                    output += "\n No kbLinks found in response"
                else:
                    output += "\n kbLinks found in response"
            else:
                if not res['kbLinks']:
                    output += "\n No kbLinks found in response"
                else:
                    output += utils.validate_kb_links(response, document_kbLinks, document_no, startTime, response_writer)

            output += utils.footer(output)
            output_writer.write(output)
            output = ""

        except Exception:
            print Exception.message
            print "CaseId: "+str(document['SRID'])
            print(traceback.format_exc())
        print "\nProcessing completed document no: "+str(document_no)
        queue.task_done()


def worker_func_kblinks(utils, queue, output_writer, response_writer):
    while True:
        data = queue.get()
        document = data[0]
        document_no = data[1]
        startTime = data[2]
        output = ""
        try:
            output += utils.header(document, document_no, "caseId")
            response = utils.request(document, "caseId")
            res = json.loads(response.text)
            output += "\n\n\n*******************************************"
            output += "\nVerifying kbLinks..."
            output += "\n*******************************************"
            if document is None:
                output += "\n No kblinks found in document"
                if not res['kbLinks']:
                    output += "\n No kbLinks found in response"
                else:
                    output += "\n kbLinks found in response"
            else:
                if not res['kbLinks']:
                    output += "\n No kbLinks found in response"
                else:
                    output += utils.validate_kb_links(response, document, document_no, startTime, response_writer)

            output += utils.footer(output)
            output_writer.write(output)
            output = ""
        except Exception:
            print Exception.message
            print "CaseId: "+str(document['caseId'])
            print(traceback.format_exc())
        print "\nProcessing completed document no: "+str(document_no)
        queue.task_done()


def worker_func_attachments(utils, queue, output_writer, response_writer):
    while True:
        data = queue.get()
        document = data[0]
        document_no = data[1]
        startTime = data[2]
        output = ""
        try:
            output += utils.header(document, document_no, "caseId")
            response = utils.request(document, "caseId")
            res = json.loads(response.text)
            output += "\n\n\n*******************************************"
            output += "\nVerifying attachments..."
            output += "\n*******************************************"
            if document is None:
                output += "\n No attachments found in document"
                if not res['kbLinks']:
                    output += "\n No attachments found in response"
                else:
                    output += "\n attachments found in response"
            else:
                if not res['attachments']:
                    output += "\n No attachments found in response"
                else:
                    output += utils.validate_sr_attachments(response, document, document_no, startTime, response_writer)
            output += utils.footer(output)
            output_writer.write(output)
            output = ""
        except Exception:
            print Exception.message
            print "CaseId: "+str(document['caseId'])
            print(traceback.format_exc())
        print "\nProcessing completed document no: "+str(document_no)
        queue.task_done()


if SR == 1:
    client = MongoClient('10.219.48.134', 27017)
    #client = MongoClient('192.168.56.101', 27017)
    db = client['ImportedEvents']
    collection = db['srDetails']
    documents = collection.find(no_cursor_timeout=True)[DOCUMENT_NO:DOCUMENT_NO_END]
    #documents = collection.find({'caseId':'2011-0525-T-0334'})
    ofile = open('DateFileVerificationDetails-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.csv', "wb")
    response_writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    row = ["SNo", "RequestTime", "CaseID", "KafkaJSON", "APIResponseStatus", "Elapsed", "DataStatus", "TimeTakenToCompleteRequestNProcess"]
    response_writer.writerow(row)
    threads = []
    output_writer = open('DateFileVerificationMatch-Output-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.txt', "wb")
    enclosure_queue = Queue()
    utils = Utils()
    # Set up some threads to fetch the enclosures
    for i in range(100):
        worker = Thread(target=worker_func_sr, args=(utils, enclosure_queue, output_writer, response_writer))
        worker.setDaemon(True)
        worker.setName("Thread-"+str(i))
        worker.start()

    # Download the feed(s) and put the enclosure URLs into the queue.
    for document in documents:
        DOCUMENT_NO += 1
        try:
            data = [document, DOCUMENT_NO, datetime.datetime.now()]
            enclosure_queue.put(data)
        except Exception:
            print Exception.message
            print "CaseId: "+str(document['SRID'])
            print(traceback.format_exc())

    print "\nDocuments in queue: "+str(enclosure_queue.qsize())
    # Now wait for the queue to be empty, indicating that we have processed all of the downloads.
    print '*** Main thread waiting'
    enclosure_queue.join()
    print '*** Done'

if KBLINKS == 1:
    client = MongoClient('10.219.48.134', 27017)
    #client = MongoClient('192.168.56.101', 27017)
    db = client['ImportedEvents']
    collection = db['srKbLink-new']
    documents = collection.find(no_cursor_timeout=True)[DOCUMENT_NO:DOCUMENT_NO_END]
    #documents = collection.find({'caseId':'2011-0525-T-0334'})
    ofile = open('reports/srkblinks/DateFileVerificationDetails-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.csv', "wb")
    response_writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    row = ["SNo", "RequestTime", "CaseID", "KafkaJSON", "APIResponseStatus", "Elapsed", "DataStatus", "TimeTakenToCompleteRequestNProcess"]
    response_writer.writerow(row)
    threads = []
    output_writer = open('reports/srkblinks/DateFileVerificationMatch-Output-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.txt', "wb")
    enclosure_queue = Queue()
    utils = Utils()
    # Set up some threads to fetch the enclosures
    for i in range(10):
        worker = Thread(target=worker_func_kblinks, args=(utils, enclosure_queue, output_writer, response_writer))
        worker.setDaemon(True)
        worker.setName("Thread-"+str(i))
        worker.start()

    # Download the feed(s) and put the enclosure URLs into the queue.
    for document in documents:
        DOCUMENT_NO += 1
        try:
            data = [document, DOCUMENT_NO, datetime.datetime.now()]
            enclosure_queue.put(data)
        except Exception:
            print Exception.message
            print "CaseId: "+str(document['caseId'])
            print(traceback.format_exc())

    print "\nDocuments in queue: "+str(enclosure_queue.qsize())
    # Now wait for the queue to be empty, indicating that we have processed all of the downloads.
    print '*** Main thread waiting'
    enclosure_queue.join()
    print '*** Done'


if ATTACHMENTS == 1:
    client = MongoClient('10.219.48.134', 27017)
    #client = MongoClient('192.168.56.101', 27017)
    db = client['ImportedEvents']
    collection = db['srAttachements-new']
    documents = collection.find(no_cursor_timeout=True)[DOCUMENT_NO:DOCUMENT_NO_END]
    #documents = collection.find({'caseId':'2011-0525-T-0334'})
    ofile = open('reports/srAttachements/DateFileVerificationDetails-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.csv', "wb")
    response_writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    row = ["SNo", "RequestTime", "CaseID", "KafkaJSON", "APIResponseStatus", "Elapsed", "DataStatus", "TimeTakenToCompleteRequestNProcess"]
    response_writer.writerow(row)
    threads = []
    output_writer = open('reports/srAttachements/DateFileVerificationMatch-Output-'+str(DOCUMENT_NO)+'-'+str(DOCUMENT_NO_END)+'.txt', "wb")
    enclosure_queue = Queue()
    utils = Utils()
    # Set up some threads to fetch the enclosures
    for i in range(10):
        worker = Thread(target=worker_func_attachments, args=(utils, enclosure_queue, output_writer, response_writer))
        worker.setDaemon(True)
        worker.setName("Thread-"+str(i))
        worker.start()

    # Download the feed(s) and put the enclosure URLs into the queue.
    for document in documents:
        DOCUMENT_NO += 1
        try:
            data = [document, DOCUMENT_NO, datetime.datetime.now()]
            enclosure_queue.put(data)
        except Exception:
            print Exception.message
            print "CaseId: "+str(document['caseId'])
            print(traceback.format_exc())

    print "\nDocuments in queue: "+str(enclosure_queue.qsize())
    # Now wait for the queue to be empty, indicating that we have processed all of the downloads.
    print '*** Main thread waiting'
    enclosure_queue.join()
    print '*** Done'