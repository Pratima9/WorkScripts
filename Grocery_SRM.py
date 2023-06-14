#Script by Pratima.U
#Date: 14 June 2023
import requests
from datetime import datetime, timedelta
import time
import string
import random
import csv

def service_event():
    #API to push grocery payload to CL for beat Id creation
    return "http://10.24.1.6:80/eventservice/event"

#Function to fetch random characters
def random_char(y):
    return ''.join(random.choice(string.ascii_letters) for x in range(y))

f1 = open("Failed Ids.txt", "a")
#File data.csv contains comma seperated ServiceRequestId and MerchantReferenceId
with open("data.csv", 'r') as file:
    csvreader = csv.reader(file)
    for row in csvreader:
        srId=row[0]
        mrfId=row[1]
        print(srId)
        body = '{"requestId":"RtdFromFc-9b65100a-05dc-4ed9-8c78-:ranChar","eventName":"RtdFromFc","eventSource":"SRMS","clientId":"flipkart","entityId":"srId","entityIdType":"SR","eventContext":{"mode":"1.0.0"},"eventData":{"active":null,"referenceIdVersion":null,"externalEntityIdentifier":null,"serviceRequestId":srId,"clientId":"flipkart","clientServiceRequestId":"mrfId","serviceRequestType":"GROCERY","serviceRequestVersion":"1.0.0","dataVersion":0,"bookingId":null,"fulfillmentUnitType":"HANDOVER_GROUP","tier":"Regular","bundleId":null,"requestId":null,"serviceStartDate":null,"serviceCompletionDate":null,"expectedCompletionDate":null,"actualCompletionDate":null,"serviceRequestHold":null,"status":"RtdFromFc","priority":null,"enterprise":null,"serviceRequestData":{},"trackingData":{"active":null,"referenceIdVersion":null,"externalEntityIdentifier":null,"orchestratorTaskId":null,"status":"RtdFromFc","location":null,"time":"2023-06-14T03:06:55+05:30","originSystem":"Orchestrator","trackingAttributes":null,"plannerTaskId":null,"user":null,"note":null,"reason":null,"serviceRequestId":srId,"eventAttributes":{},"uniqueIdHash":"09433bcf62c7ec6965d2e908eaaeb7de","isInternal":false},"identityMappings":[],"oldEntities":{},"shipmentType":"IncomingShipment","uniqueIdHash":"78f6e746c5114c252cd4690bee24223a"}}'
        #Replace ServiceRequestId and MerchantReferenceId in the body
        body = body.replace('srId', srId)
        body = body.replace('mrfId', mrfId)
        body = body.replace(':ranChar', str(random_char(10)))
        print(body)
        #Add headers
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["X_RESTBUS_MESSAGE_ID"] = "ea3ad741c6daac1c0153bbc1"+str(random_char(5))
        headers["X_EVENT_NAME"] = "RtdFromFc"
        headers["X_BS_TYPE"] = "GROCERY"
        headers["X_FU_TYPE"] = "HANDOVER_GROUP"
        headers["X_BS_VERSION"] = "1.0.0"

        try:
            response = requests.post(service_event(), headers=headers, data=body).text
            print(response)
            print("==========================================")

        except ValueError as er:
            print('error ' + str(er))
            f1.write(str(srId) + "  --  exception from orch ")
            f1.write('\n')
            time.sleep(1)

print("done")
f1.close()
