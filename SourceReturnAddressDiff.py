#!/usr/bin/python
import mysql.connector
import subprocess
import requests
import json
from datetime import datetime, timedelta
import time
import string
import random

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
        my_dict["tracking_id"] = message["serviceRequest"]["serviceRequestData"]["pickupVendor"]["trackingIds"][0]
        my_dict["SourceAddressId"] = message["serviceRequest"]["serviceRequestData"]["source"]["address"]["addressId"]
        my_dict["ReturnAddressId"] = message["serviceRequest"]["serviceRequestData"]["returnLocation"]["address"]["addressId"]
    return my_dict

tracking_ids = ["MYEC1001456384"]
naive_dt = datetime.now() - timedelta(hours=5, minutes=30)
naive_dt = naive_dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")

f = open("trackingids.txt", "a")
f1 = open("shipmentNotFound.txt", "a")
for trackingId in tracking_ids:
    try:
        srId = getSRId(trackingId)
        trackResponse = getTrackResponse(srId)
        if "statusCode" in trackResponse:
            details = getDetails(trackResponse)
            if details['SourceAddressId']!=details['ReturnAddressId']:
                print(details['tracking_id'])
    except ValueError as er:
        print('error ' + str(er))
        f1.write(str(trackingId) + "  --  exception from shipping ")
        f1.write('\n')

print("done")
f.close()
f1.close()
