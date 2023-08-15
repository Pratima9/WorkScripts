import requests
import csv
import json
import time ,datetime
#from pytz import timezone
import sys

#Author - bikrant.sahoo@
#Run it either in local or machine

filename = sys.argv[1]

class mydict(dict):
    def __str__(self):
        return json.dumps(self)


with open(filename, 'r') as csvfile:
    for id in csv.reader(csvfile, delimiter=','):
        print "Tracking_id:", id[0]
        url="http://10.24.1.47/shipments/e2e/updateTrackStatus?requestType=TRACKE2E"
        payload = {}
        data = {}
        payload['shipments'] = data
        data["status"] = id[3]
        data["trackingId"] = id[0]
        data["statusLocation"] = ""
        data['receivedBy'] = "string"
        data['remarks']  = ""
        #data['remarks'] = "os part of oncall data fix"
        #data['remarks'] = "as part of datafixon request of arun.kmr@flipkart.com"
        data['creationDate'] = id[2]
        #status_date = datetime.strptime(id[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone('UTC')).strftime("%Y-%m-%dT%H:%M:%S+05:30")
        status_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S+05:30')
        data['shipmentTimeStamp'] = status_date
        data['eventTimeStamp'] = status_date
        data['statusDescription'] = ""
        data['serviceRequestId'] = id[1]
        payload['parentSrId'] = id[1]
        payload['clientId'] = "oncall"

        headers = {}
        headers['content-type'] = 'application/json'
        headers['X_CLIENT_ID'] = 'oncall'

        payload = mydict(payload)
        headers = mydict(headers)


        print payload
        print headers

        r = requests.post(url, data=json.dumps(payload), headers=headers)
        #print r.status_code
