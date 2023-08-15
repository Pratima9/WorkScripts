#!/usr/bin/python
import mysql.connector
import subprocess
import requests
import json
from datetime import datetime, timedelta
import time
import string
import random

def get_orchestrator_url():
    # return "http://10.24.4.32/api/service-requests/{}".format(sr_id)
    # return "https://api.ekartlogistics.com/shipments/updateStatusLarge"
    # return "http://localhost:36050/shipments/updateStatusLarge"
    return "http://10.24.1.7/orchestrator/consume/stateEvent"


def getSRId(trackingId):
    srId_url = "http://10.24.1.4/api/secondary-index?value=" + str(trackingId) + "&key=TrackingId"
    headers = {}
    headers["Content-Type"] = "application/json"
    response = requests.get(srId_url, headers)
    srId = response.json()["payload"]
    if ('com.ekart.srms.commons.exceptions.NotFoundException' == srId):
        raise ValueError('NotFoundException')
    return response.json()["payload"]


def getTrackResponse(srId):
    trackUrl = "http://10.24.1.4/api/service-requests/track?serviceRequestId=" + str(srId)
    headers = {}
    headers["Content-Type"] = "application/json"
    response = requests.get(trackUrl, headers)
    return response.json()


def getDetails(trackResponse):
    my_dict = {}
    for message in trackResponse["payload"]:
        my_dict["merchant_code"] = message["serviceRequest"]["clientId"]
        my_dict["merchant_reference_id"] = message["serviceRequest"]["clientServiceRequestId"]
        my_dict["vendorID"] = my_dict["type"] = message["serviceRequest"]["serviceRequestData"]["vendors"][0]["vendorId"]
        my_dict["vendorCode"] = message["serviceRequest"]["serviceRequestData"]["vendors"][0]["vendorCode"]
        my_dict["name"] = message["serviceRequest"]["serviceRequestData"]["vendors"][0]["name"]
        my_dict["tracking_id"] = message["serviceRequest"]["serviceRequestData"]["vendors"][0]["trackingIds"][0]
        my_dict["SR_Status"] = message["serviceRequestsTrackData"][-1]
        my_dict["SR_Previous_State"] = message["serviceRequestsTrackData"][-2]["status"]
    return my_dict


def random_char(y):
    return ''.join(random.choice(string.ascii_letters) for x in range(y))


tracking_ids = ["MYER1000724659"]
naive_dt = datetime.now() - timedelta(hours=5, minutes=30)
naive_dt = naive_dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
print(naive_dt)

f = open("trackingids.txt", "a")
f1 = open("shipmentNotFound.txt", "a")
for trackingId in tracking_ids:
    try:
        srId = getSRId(trackingId)
        print(srId)
        trackResponse = getTrackResponse(srId)
        # print trackResponse
        #changesrstate(srId)
        if "statusCode" in trackResponse:
            # print trackResponse
            details = getDetails(trackResponse)
            # print details
            #srms_vtgate={'user':'mysql_user', 'password':'mysql_password', 'host':'10.52.83.104', 'database':'srms','port':4444}
            #query1='update sr set status="{}" where sr_id=srId'.format(str(details["SR_Previous_State"]))
            query2='update service_requests set status=str(details["SR_Previous_State"]) where sr_id=srId'
            query3='delete from sr_tracking_data where shard_key=srId and status="SentToAssets"'
            update_query = "mysql -umysql_user -P4444 -pmysql_password -h 10.52.83.104 srms -e 'update sr set status=\"{}\" where sr_id=srId'".format(str(details["SR_Previous_State"]))
            process = subprocess.Popen(update_query, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            status = process.returncode
            if status != 0:
                raise Exception("Error updating SR data: {}".format(stderr))
            body = '{"requestId":"e39d1377-833e-4477-a09b-a01938d550j-:ranChar","srId"::srId,"status":":details.SR_Previous_State",' \
                   '"smIdToStateIdLookup":{"NONFA_FORWARD_3PL_HANDOVER:2.0.0":"SentToAssets","NONFA_FORWARD_E2E_EKART:2.0.0":"SentToAssets","NONFA_RVP_E2E_EKART:all":"SentToAssets",' \
                   '"NONFA_RVP_3PL_HANDOVER:all":"SentToAssets","FA_FWD_EKART_SHIPMENT:all":"SentToAssets","FA_FORWARD_3PL_SHIPMENT:all":"SentToAssets",' \
                   '"FA_RVP_EKART_SHIPMENT:all":"SentToAssets","FA_RVP_3PL_HANDOVER_SHIPMENT:all":"SentToAssets","EXT_RVP_EKART:all":"SentToAssets"},' \
                   '"eventMap":{"com.ekart.orchestrator.schema.events.shipping.shippingsubmitted.ShippingSubmitted":{"ekart_service_request_id":":srId",' \
                   '"merchant_reference_id":":details.merchant_reference_id","vendor_id":":details.vendorID","vendor_tracking_id":":details.tracking_id","vendor_name":":details.name","vendor_code":":details.vendorCode"}},' \
                   '"trackingData":{"status":":details.SR_Previous_State","originSystem":"fkl-shipping","uniqueIdHash":"a91cf4b05898ad1f82490047d3d386c2"},"suppressError":false,' \
                   '"originSystem":"fkl-shipping","name":":details.SR_Previous_State"}'
            body = body.replace(':srId', srId)
            body = body.replace(':details.merchant_reference_id', str(details["merchant_reference_id"]))
            body = body.replace(':details.vendorID', str(details["vendorID"]))
            body = body.replace(':details.tracking_id', str(details["tracking_id"]))
            body = body.replace(':details.name', str(details["name"]))
            body = body.replace(':details.vendorCode', str(details["vendorCode"]))
            body = body.replace(':ranChar', str(random_char(5)))
            body = body.replace(':details.SR_Previous_State',str(details["SR_Previous_State"]))

            # print ("==========================================")
            print(body)
            headers = {}
            headers["Content-Type"] = "application/json"

            try:
                response = requests.post(get_orchestrator_url(), headers=headers, data=body).text
                print(response)
                response = json.loads(response)
                print("==========================================")
                print(response['isSuccessful'])
                if (response['isSuccessful']==False):
                    f1.write(str(trackingId) + "  --  state transition exception from orch ")
                    f1.write('\n')

            except ValueError as er:
                print('error ' + str(er))
                f1.write(str(trackingId) + "  --  exception from orch ")
                f1.write('\n')

            f.write(str(trackingId))
            f.write('\n')
            time.sleep(1)
    except ValueError as er:
        print('error ' + str(er))
        f1.write(str(trackingId) + "  --  exception from shipping ")
        f1.write('\n')

print("done")
f.close()
f1.close()
