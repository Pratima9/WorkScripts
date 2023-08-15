from datetime import date
import mysql.connector
import json
import sys
import requests
import csv
import subprocess
import os
from threading import Thread


BATCH_SIZE=100
def mysql_connect(filename,subject,body):
    query="""
        SELECT
        sr_data_non_queryable.blobs,
        'Forward',
        IFNULL(service_requests.tier,'NA') ,
        IFNULL(service_requests.service_completion_date,'NA') ,
        IFNULL(service_requests.updated_at,'NA') ,
        CASE
        WHEN service_requests.sr_type = 'FA_%' THEN 'FBF'
        ELSE 'NFBF'
        END ,
        #"First inscan hub"

        IFNULL((SELECT sr_tracking_data.location FROM sr_tracking_data WHERE sr_tracking_data.status='InscannedAtFacility' AND sr_tracking_data.origin_system='HMS'  AND
        sr_tracking_data.shard_key=service_requests.sr_id order by sr_tracking_data.created_at desc limit 1
        ),'NA'),

        #"first inscan time"
        IFNULL((SELECT sr_tracking_data.time FROM sr_tracking_data WHERE sr_tracking_data.status='InscannedAtFacility' AND sr_tracking_data.origin_system='HMS'  AND
        sr_tracking_data.shard_key=service_requests.sr_id order by sr_tracking_data.created_at desc limit 1
        ),'NA'),

        # "LATEST DH "
        IFNULL((SELECT sr_tracking_data.location FROM sr_tracking_data WHERE sr_tracking_data.status='InscannedAtDH'  AND
        sr_tracking_data.shard_key=service_requests.sr_id order by sr_tracking_data.created_at desc limit 1
        ),'NA'),

        #"Repromise"
        IFNULL((SELECT sr_tracking_data.notes FROM sr_tracking_data WHERE sr_tracking_data.notes='REPROMISE_REQUEST'  AND
        sr_tracking_data.shard_key=service_requests.sr_id order by sr_tracking_data.created_at desc limit 1
        ),'NO_PROMISE_CHANGE')
        FROM service_requests, sr_data_non_queryable
        WHERE sr_data_non_queryable.shard_key=service_requests.sr_id
        AND DATE(service_requests.service_completion_date) = DATE(now())
        AND service_requests.sr_type in ('FA_FORWARD_E2E_EKART','FWD_E2E_EKART','NONFA_FORWARD_E2E_EKART','FA_FORWARD_3PL_HANDOVER','FA_RVP_3PL_HANDOVER','FA_RVP_E2E_EKART','NONFA_FORWARD_3PL_HANDOVER','NONFA_FORWARD_E2E_3PL','NONFA_RVP_3PL_HANDOVER','NONFA_RVP_E2E_EKART')
        AND service_requests.client_id="MYE"
        AND lower(service_requests.status) not in('ShipmentDelivered','ShipmentRtoCompleted','PickupCancelled','ShipmentRtoConfirmed','ShipmentUndeliveredAttempted','ShipmentOutForDelivery');
        """


    if os.path.exists(filename):
        os.remove(filename)
    fill_headers(filename)
    shards =[
        {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.53.168.165', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.48.66.200', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.52.149.154', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.49.53.14', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.54.130.254', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.52.131.129', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.54.86.46', 'database':'srms','port':17001},
        # {'user':'cron_user', 'password':'e2e-orchestrator', 'host':'10.48.2.113', 'database':'srms','port':17001}
    ]

    for shard in shards:
        print("Querying:    "+str(shard["host"]))
        dummmy_filename = "/mnt/archive/MYE/"+shard["host"]+".csv"
        if os.path.exists(dummmy_filename):
            os.remove(dummmy_filename)
        conn= mysql.connector.connect(**shard)
        cursor = conn.cursor()
        cursor.execute(query)
        myresult = cursor.fetchall()
        with open(dummmy_filename, 'a', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(myresult)
            csvfile.close()
        print("Sucessfully written to:  "+dummmy_filename)
    thread0 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[0]["host"]+".csv"))
    # thread1 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[1]["host"]+".csv"))
    # thread2 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[2]["host"]+".csv"))
    # thread3 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[3]["host"]+".csv"))
    # thread4 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[4]["host"]+".csv"))
    # thread5 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[5]["host"]+".csv"))
    # thread6 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[6]["host"]+".csv"))
    # thread7 = Thread(target=get_data,args=(filename,"/mnt/archive/MYE/"+shards[7]["host"]+".csv"))
    print("Thread Execution Started For Shard:   "+str(shards[0]["host"]))
    thread0.start()
    print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[0]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[1]["host"]))
    # thread1.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[1]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[2]["host"]))
    # thread2.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[2]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[3]["host"]))
    # thread3.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[3]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[4]["host"]))
    # thread4.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[4]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[5]["host"]))
    # thread5.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[5]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[6]["host"]))
    # thread6.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[6]["host"]))
    # print("Thread Executaion Started For Shard:   "+str(shards[7]["host"]))
    # thread7.start()
    # print("Waiting For Thread To Complete Task For  Shard:   "+str(shards[7]["host"]))
    thread0.join()
    print("Thread Executaion Completed For Shard:   "+str(shards[0]["host"]))
    # thread1.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[1]["host"]))
    # thread2.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[2]["host"]))
    # thread3.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[3]["host"]))
    # thread4.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[4]["host"]))
    # thread5.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[5]["host"]))
    # thread6.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[6]["host"]))
    # thread7.join()
    # print("Thread Executaion Completed For Shard:   "+str(shards[7]["host"]))
    send_mail(subject,body,filename)
    print("Email sent")

def fetch_data(shipment_id):
    query1="select vendor_tracking_id,state from shipments where merchant_reference_id in ('{}')".format(shipment_id)
    shipping_cred={'user':'v-ship-5YdbZ84Dx', 'password':'A1a-60AW4GfAqF67PTdR', 'host':'prod-hyd.shipping-db.appslave.shipping.fkcloud.in', 'database':'shipping'}
    conn= mysql.connector.connect(**shipping_cred)
    cursor = conn.cursor()
    cursor.execute(query1)
    myresult1 = cursor.fetchall()
    return myresult1


def fill_headers(filename):
    columns="Shipment ID,Vendor Tracking Id,Shipment Type,Shipment Tier Type,Source PinCode,Delivery PinCode,Customer Promise Date,Type,Price/Shipment Value,Latest Status,Latest Update Date Time,Seller Type,First Received Facility Id,First Receive Date Time,Repromised?"
    with open (filename,'w', newline='', encoding='utf-8') as file:
        file.write(str(columns))
        file.write("\n")
        file.close()

def get_data(filename,dummmy_filename):
    print("Processing   "+dummmy_filename)
    excluded_status_list=['rto_confirmed','rto_completed','delivered','out_for_delivery','undelivered_attempted','undelivered_unattempted','trigger_ndr','pickup_cancelled','misrouted','cancel_ndr','rto_cancelled','lost','damaged','rto_out_for_delivery','rto_undelivered_attempted','rto_handover_completed','partial_lost','partial_damage','undelivered_completed','rto_received','reattempt_received','reattempt_accepted','reattempt_cancelled','reschedule_completed','reschedule_declined','warehouse_breach','reschedule_requested','delivery_update','reattempt_initiated','reattempt_processing','reattempt_processing','reattempt_failed','reattempt_confirmed','reschedule','ndr_expired','shipment_not_received','change_delivery_slot_initiated','change_delivery_slot_confirmed','change_delivery_slot_failed','valid_selfship_shipment']
    shipments_list =[]
    count = 0
    with open(dummmy_filename,'r',encoding='utf_8') as csvfile:
        csv.field_size_limit(100000000)
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            #try:
            # print(json.loads(row[0]).get('shipment')['shipmentId'])
            #except:
            #print("unable to print the shipmentID for row: "+row)
            if(json.loads(row[0]).get('shipment') and row[8] == 'NA'):
                shipment_id=str(json.loads(row[0])['shipment']['shipmentId'])
                shipment_type=row[1]
                shipment_tier_type=row[2]
                source_pincode=str(json.loads(row[0])['source']['address']['pincode'])
                delivery_pincode=str(json.loads(row[0])['customer']['shippingAddress']['pincode'])
                customer_promise_date=row[3]
                type=str(json.loads(row[0])['shipment']['payment']['paymentDetails'][0]['type'])
                price=str(json.loads(row[0])['shipment']['payment']['totalAmount']['value'])
                update_time=row[4]
                seller_type=row[5]
                first_received_mh=row[6]
                first_received_date_time=row[7]
                #delivery_hub_Assigned=row[8]
                repromised=row[9]
                #vendor_tracking_id="NA"
                shipping_fetch=fetch_data(1395796389)
                vendor_tracking_id=shipping_fetch[0][0]
                shipment =[]
                status=shipping_fetch[0][1]
                if status in ("returned_to_seller","not_received"):
                    continue
                else:
                    shipment.append(shipment_id)
                    shipment.append(vendor_tracking_id)
                    shipment.append(shipment_type)
                    shipment.append(shipment_tier_type)
                    shipment.append(source_pincode)
                    shipment.append(delivery_pincode)
                    shipment.append(customer_promise_date)
                    shipment.append(type)
                    shipment.append(price)
                    shipment.append(status)
                    shipment.append(update_time)
                    shipment.append(seller_type)
                    shipment.append(first_received_mh)
                    shipment.append(first_received_date_time)
                    #shipment.append(current_location)
                    shipment.append(repromised)
                    shipments_list.append(shipment)
                    count = count +1
                    if count == BATCH_SIZE:
                        writeTocsv(filename, shipments_list)
                        count = 0
                        shipments_list=[]


    print("Successfully Processed:   "+dummmy_filename)
    #send_mail(subject, body, filename)

def writeTocsv(filename, shipments_list):
    with open(filename, 'a', newline="\n" , encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(shipments_list)

def send_mail(subject, body, filename):
    print("Sending Email")
    zip_file=filename+'.gz'
    if os.path.exists(zip_file):
        os.remove(zip_file)
    zip_command = 'gzip '+ filename
    subprocess.call(zip_command, shell=True)
    mail_command = '(echo "' + body + '"'
    mail_command += ' && uuencode ' + filename+'.gz' + ' ' + filename+'.gz'
    mail_command += ') | mail -s "' + subject + '" pratima.u@flipkart.com'
    subprocess.call(mail_command, shell=True)

filename = "/mnt/archive/MYE/result/"+str(date.today())+"_MYE_mh_lh_cron_report_new.csv"
SUBJECT="MH_LH_CRON_MYE_"+str(date.today())
BODY="Hi,\n Please find Mh-Lh cron data for MYE"
mysql_connect(filename,SUBJECT,BODY)
#send_mail(SUBJECT,BODY,filename)
