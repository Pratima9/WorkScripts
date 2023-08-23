#!/bin/bash
set -x

CURR_DATE=$(date +"%b-%d")
CURR_TIME=$(date +"%Y-%m-%d_%H-%M-%S")
DATE=`date +"%F" `
CURR_DATE=`date +"%b-%d"`
LAST_DATE=`date +\%Y-\%m-\%d -d "-1 days"`

CWD='/mnt/archive/ext/playground/'

srms_user="srms_ro"
srms_pwd="4j^WbaSmd1"
srms_db="srms"
srms_ip="10.24.40.139"
srms_port="4000"

export MAILFROM="ext-oncall <ekart-ext-oncall@flipkart.com>"
export MAILTO="pratima.u@flipkart.com"
export SUBJECT="Large billing report | ${CURR_DATE}"

echo "lets start!!! shall we :)"

echo "started fetching the total SR ids for a month in a while loop"


srms_query="select distinct client_service_request_id , sr_id from service_requests s where s.client_id not in ('flipkart') and s.sr_type in ('NONFA_FORWARD_LARGE_E2E_EKART','NONFA_RVP_LARGE_E2E_EKART')
AND s.created_at between '2023-08-01 00:00:00' and '2023-08-15 23:59:59';"

echo "$srms_query" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' > $CWD/srms_total_temp.csv


awk -F ',' '{print $2}' $CWD/srms_total_temp.csv > $CWD/srms_total.csv
input_file_name1=$CWD/srms_total.csv
comma_separated_file_input1="commaseparated_data_$DATE"

awk -F "<>" '{print $1}' $input_file_name1 |awk '{printf("'"'"'%s'"'"',", $0)} END{print ""}' |sed -e 's/^/'\('/g' |sed -e 's/$/'\)'/g' |sed 's/\,)/\)/' > $comma_separated_file_input1

xquery="select distinct SUBSTRING_INDEX(SUBSTRING_INDEX(blobs, '\"name\":\"vendor_tracking_id\",\"type\":\"string\",\"value\":\"', -1), '\"', 1) as vendor_tracking_id from sr_data_non_queryable where shard_key IN $(cat $comma_separated_file_input1);"

echo "$xquery" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db" | uniq | sed 's|\t|,|g' >$CWD/srid_list.csv


awk '!/vendor_tracking_id/' $CWD/srid_list.csv  > $CWD/srid_list1.csv

echo "Merchant_ID,LR_ID,Tracking ID,Client_ID,Shipment_Type,Shipment_Status,Shipment_Status_Date,Payment_Type,Docket_ID,Docket_Status,Length,Breadth,Height,item_weight,vol_weight,Shipment_weight,Source_Pincode,Destination_Pincode,Source_State,Destination_State,Shipment_Value,amount_to_collect" >"$CWD/output.csv"

while IFS= read -r tracking_id; do
	curl_output=$(curl --location --request GET "http://10.24.1.7/service-requests/internal/track?trackingId=${tracking_id}")
	echo "$curl_output" >$CWD/response.json

	service_request_type=$(jq -r '.payload.serviceRequest.serviceRequestType' $CWD/response.json)
	if [[ "$service_request_type" == "NONFA_FORWARD_LARGE_E2E_EKART" ]]; then
		Shipment_Type="FORWARD"
	else
		Shipment_Type="REVERSE"
	fi

	Shipment_Status=$(jq -r '.payload.serviceRequest.status' $CWD/response.json)
	if [[ $Status == "OutForPickupEvent" && $Shipment_Type == "REVERSE" ]]; then
		Current_Status="out_for_pickup"
	elif [[ $Status == "OutForPickupEvent" ]]; then
		Current_Status="pickup_out_for_pickup"
	elif [[ $Status == "PickupRescheduledEvent" ]]; then
		Current_Status="pickup_reattempt"
	elif [[ $Status == "PickupReceived" ]]; then
		Current_Status="shipment_pickup_complete"
	elif [[ $Status == "PickupCancelled" ]]; then
		Current_Status="pickup_cancelled"
	elif [[ $Status == "InScanAtHub" && $Shipment_Type == "REVERSE" ]]; then
		Current_Status="return_received"
	elif [[ $Status == "InScanAtHub" ]]; then
		Current_Status="received"
	elif [[ $Status == "InscannedAtDH" ]]; then
		Current_Status="received_at_dh"
	elif [[ $Status == "ShipmentOutForDelivery" ]]; then
		Current_Status="shipment_out_for_delivery"
	elif [[ $Status == "ShipmentUndeliveredAttempted" ]]; then
		Current_Status="shipment_undelivered_attempted"
	elif [[ $Status == "ShipmentDelivered" ]]; then
		Current_Status="shipment_delivered"
	elif [[ $Status == "ShipmentLost" ]]; then
		Current_Status="shipment_lost"
	elif [[ $Status == "ShipmentRtoConfirmed" ]]; then
		Current_Status="shipment_rto_confirmed"
	elif [[ $Status == "ShipmentRtoCompleted" ]]; then
		Current_Status="shipment_rto_completed"
	elif [[ $Status == "RtoCancelled" ]]; then
		Current_Status="shipment_rto_cancelled"
	elif [[ $Status == "PickupCancel" ]]; then
		Current_Status="pickup_cancelled"
	elif [[ $Status == "NotPickedAttemptedEvent" ]]; then
		Current_Status="pickup_not_picked_attempted"
	elif [[ $Status == "NotPickedNotAttemptedEvent" ]]; then
		Current_Status="pickup_not_picked_unattempted"
	elif [[ $Status == "PickedComplete" ]]; then
		Current_Status="pickup_done"
	elif [[ $Status == "ReturnOutForDelivery" ]]; then
		Current_Status="return_out_for_delivery"
	elif [[ $Status == "ReturnUndeliveredAttempted" ]]; then
		Current_Status="return_undelivered_attempted"
	elif [[ $Status == "ShipmentRvpCompleted" ]]; then
		Current_Status="return_delivered"
	else
		Current_Status="Created"
	fi

	Shipment_Status_Date=$(jq -r '.payload.serviceRequest.serviceStartDate' $CWD/response.json)
	Payment_Type=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.payment.paymentDetails[].type' $CWD/response.json)
	Docket_ID=$(jq -r '.payload.serviceRequest.clientServiceRequestId' $CWD/response.json)
	Docket_Status=$(jq -r '.payload.serviceRequest.status' $CWD/response.json)
	Length=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.length.value' $CWD/response.json)
	Breadth=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.breadth.value' $CWD/response.json)
	Height=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.height.value' $CWD/response.json)
	item_weight=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentItems[0].item.itemWeight.value' $CWD/response.json)
	vol_weight=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentDimension.volumetricWeight.value' $CWD/response.json)
	Shipment_weight=$(jq '.payload.serviceRequest.serviceRequestData.shipment.shipmentWeight.value' $CWD/response.json)
	Source_Pincode=$(jq -r '.payload.serviceRequest.serviceRequestData.source.address.pincode' $CWD/response.json)
	Destination_Pincode=$(jq -r '.payload.serviceRequest.serviceRequestData.destination.address.pincode' $CWD/response.json)
	Source_State=$(jq -r '.payload.serviceRequest.serviceRequestData.source.address.state' $CWD/response.json)
	Destination_State=$(jq -r '.payload.serviceRequest.serviceRequestData.destination.address.state' $CWD/response.json)
	Shipment_Value=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.shipmentItems[0].item.payment.totalAmount.value' $CWD/response.json)
	LR_ID=${tracking_id}
	Merchant_ID=${tracking_id}
	Client_ID=$(jq -r '.payload.serviceRequest.clientId' $CWD/response.json)
	amount_to_collect=$(jq -r '.payload.serviceRequest.serviceRequestData.shipment.shipmentItems[].totalPayment.amountToCollect.value' $CWD/response.json)

	echo -e "$Merchant_ID,$LR_ID,$tracking_id,$Client_ID,$Shipment_Type,$Shipment_Status,$Shipment_Status_Date,$Payment_Type,$Docket_ID,$Docket_Status,$Length,$Breadth,$Height,$item_weight,$vol_weight,$Shipment_weight,$Source_Pincode,$Destination_Pincode,$Source_State,$Destination_State,$Shipment_Value,$amount_to_collect" >>"$CWD/output.csv"
done <$CWD/srid_list1.csv

awk '!/vendor_tracking_id/' $CWD/output.csv  > $CWD/output_final.csv


awk '!/sr_id/' $CWD/srms_total.csv  > $CWD/srs_final.csv

input_file_name=$CWD/srs_final.csv
comma_separated_file_input="commaseparated_data_$DATE"

awk -F "<>" '{print $1}' $input_file_name |awk '{printf("'"'"'%s'"'"',", $0)} END{print ""}' |sed -e 's/^/'\('/g' |sed -e 's/$/'\)'/g' |sed 's/\,)/\)/' > $comma_separated_file_input


pquery="SELECT DISTINCT
SUBSTRING_INDEX(SUBSTRING_INDEX(sd.blobs, '\"name\":\"vendor_tracking_id\",\"type\":\"string\",\"value\":\"', -1), '\"', 1)AS Merchant_ID,
ssh_pickup.time AS 'Pickup_Date',
ssh_inscan.time AS 'MH_In_Scan_Date',
ssh_dh_inscan.time AS 'DH_In_Scan_Date',
ssh_rto.time AS 'RTO_request_date',
ssh_delivered.time AS 'Delivered_Date',
SUBSTRING_INDEX(SUBSTRING_INDEX(blobs, '\"name\":\"shipment_movement_type\",\"type\":\"string\",\"value\":\"', -1), '\"', 1) AS zone
FROM
service_requests s
JOIN sr_tracking_data std ON s.sr_id = std.shard_key
LEFT JOIN sr_tracking_data ssh_pickup ON ssh_pickup.shard_key = s.sr_id AND ssh_pickup.status = 'PickupReceived'
LEFT JOIN sr_tracking_data ssh_inscan ON ssh_inscan.shard_key = s.sr_id AND ssh_inscan.status = 'InscannedAtFacility'
LEFT JOIN sr_tracking_data ssh_dh_inscan ON ssh_dh_inscan.shard_key = s.sr_id AND ssh_dh_inscan.status = 'InscannedAtDH'
LEFT JOIN sr_tracking_data ssh_rto ON ssh_rto.shard_key = s.sr_id AND ssh_rto.status = 'ShipmentRtoConfirmed'
LEFT JOIN sr_tracking_data ssh_delivered ON ssh_delivered.shard_key = s.sr_id AND ssh_delivered.status = 'ShipmentDelivered'
LEFT JOIN sr_data_non_queryable sd ON s.sr_id = sd.shard_key
WHERE
sd.shard_key IN $(cat $comma_separated_file_input)"

echo "$pquery" | mysql -u "$srms_user" -p"$srms_pwd" -h "$srms_ip" -P "$srms_port" "$srms_db"  | uniq | sed 's|\t|,|g' >$CWD/sr_total.csv
join -1 1 -2 1 -t, -o 1.3,1.2,1.1,1.4,2.2,2.3,2.4,2.5,2.6,1.5,1.6,2.7,1.7,1.8,1.9,1.10,1.11,1.12,1.13,1.14,1.15,1.16,1.17,1.18,1.19,1.20,1.21,1.22 <(sort -k1 $CWD/output_final.csv) <(sort -k1 $CWD/sr_total.csv)> $CWD/Large_billing_data.csv
uniq -i $CWD/Large_billing_data.csv > $CWD/temp_file && mv $CWD/temp_file $CWD/Large_billing_data.csv

awk -F ',' '!seen[$1]++' $CWD/Large_billing_data.csv > $CWD/output.csv && mv $CWD/output.csv $CWD/Large_billing_data.csv

awk '!/^Tracking ID,LR_ID,Merchant_ID,Client_ID,Pickup_Date,MH_In_Scan_Date,DH_In_Scan_Date,RTO_request_date,Delivered_Date,Shipment_Type,Shipment_Status,zone,Shipment_Status_Date,Payment_Type,Docket_ID,Docket_Status,Length,Breadth,Height,item_weight,vol_weight,Shipment_weight,Source_Pincode,Destination_Pincode,Source_State,Destination_State,Shipment_Value,amount_to_collect/' $CWD/Large_billing_data.csv > $CWD/output.csv && mv $CWD/output.csv $CWD/Large_billing_data.csv

awk 'BEGIN{print "Tracking ID,LR_ID,Merchant_ID,Client_ID,Pickup_Date,MH_In_Scan_Date,DH_In_Scan_Date,RTO_request_date,Delivered_Date,Shipment_Type,Shipment_Status,zone,Shipment_Status_Date,Payment_Type,Docket_ID,Docket_Status,Length,Breadth,Height,item_weight,vol_weight,Shipment_weight,Source_Pincode,Destination_Pincode,Source_State,Destination_State,Shipment_Value,amount_to_collect"} 1' $CWD/Large_billing_data.csv > $CWD/output.csv && mv $CWD/output.csv $CWD/Large_billing_data.csv

(echo -e " Hi Peeps,\n  The Large billing report is available for download at http://10.24.10.90/getData ") | mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO

#rm $CWD/output.csv  $CWD/data.csv $CWD/srms_total.csv  $CWD/output.csv $CWD/response.json $CWD/srms_total_temp.csv $CWD/srms_*.csv $CWD/output_temp.csv $CWD/output_tempa.csv $CWD/output_final.csv extracted.json $CWD/srs_*.csv $CWD/srs_total.csv $CWD/sr_*.csv $CWD/sr_total_temp.csv $CWD/output_final.csv $CWD/sr_total.csv $CWD/output_final_sorted.csv $CWD/sr_total_sorted.csv  commaseparated_data_$DATE $CWD/srms_total_temp.csv $CWD/temp_files $CWD/srid_list.csv $CWD/srid_list1.csv $CWD/output.csv $CWD/data* $CWD/datb*
