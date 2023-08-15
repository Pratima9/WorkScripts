#!/bin/bash

#Author - bikrant.sahoo@flipkart.com

#. /usr/share/3pl-crons/db/db_config.sh

decoded_shipping_ro_user=`cat /etc/db_config.json|jq .shipping[].SlaveUser|sed 's/"//g'`
encrypted_shipping_ro_pass=`cat /etc/db_config.json|jq .shipping[].SlavePassword|sed 's/"//g'`
shipping_ro_ip=`cat /etc/db_config.json|jq .shipping[].hosts[].slave4|sed 's/"//g'`
#shipping_ro_ip="prod-hyd.shipping-db.appslave.shipping.fkcloud.in"
decoded_shipping_db_name=`cat /etc/db_config.json|jq .shipping[].Database|sed 's/"//g'`
decoded_shipping_ro_pass=`bash /usr/share/3pl-crons/db/get_db_cred.sh  3pl-crons-readuser $encrypted_shipping_ro_pass`

CURR_TIME=`date +"%Y-%m-%d_%H-%M-%S"`
CURR_DATE=`date +"%b-%d"`

export MAILFROM="ext-oncall <ekart-ext-oncall@flipkart.com>"
export MAILTO="bikrant.sahoo@flipkart.com,ekart-ext-oncall@flipkart.com,pratima.u@flipkart.com"
export SUBJECT="Externalization RTO | ${CURR_DATE}"

query="select distinct Temp_Table.merchant_reference_id from ( select s.merchant_code,s.id,s.merchant_reference_id,s.vendor_tracking_id from shipments s join shipment_status_histories ssh on s.id = ssh.shipment_id where s.merchant_code not in ('FKMP','EMERALD','SRT','KRT','MYN','XMI') and s.state in ('pickup_reattempt','pickup_out_for_pickup') and ssh.new_status='pickup_reattempt' and s.flags in (0,16) and s.updated_at >= DATE(NOW() - INTERVAL 20 DAY) ) Temp_Table group by Temp_Table.id  having count(*) >= 5";
query1="SELECT DISTINCT Temp_Table.vendor_tracking_id,Temp_Table.merchant_reference_id FROM (SELECT s.merchant_code,s.id,s.merchant_reference_id,s.vendor_tracking_id FROM shipments s JOIN shipment_status_histories ssh ON s.id = ssh.shipment_id WHERE s.merchant_code IN ('MYS') AND s.state IN ('pickup_reattempt', 'pickup_out_for_pickup') AND ssh.new_status = 'pickup_reattempt' AND s.flags IN (0, 16) AND s.updated_at >= DATE(NOW() - INTERVAL 20 DAY)) Temp_Table GROUP BY Temp_Table.id HAVING count(*) >= 5";
squery="select distinct Temp_Table.merchant_reference_id  from ( select s.merchant_code,s.id,s.merchant_reference_id,s.vendor_tracking_id from shipments s join shipment_status_histories ssh on s.id = ssh.shipment_id   where  s.merchant_code  in ('SRT','KRT') and s.state in ('pickup_reattempt','pickup_out_for_pickup') and ssh.new_status='pickup_reattempt' and s.flags in (0,16) and s.updated_at >= DATE(NOW() - INTERVAL 10 DAY) ) Temp_Table group by Temp_Table.id  having count(*) >= 10";

cwd='/mnt/archive/'

echo "Connecting to shipping hotstore"
echo " bingo !! Connected .. "

echo "$query"|mysql -N  -u$decoded_shipping_ro_user -p$decoded_shipping_ro_pass -h$shipping_ro_ip $decoded_shipping_db_name >> $cwd/five_days.csv

e=`wc -l <$cwd/five_days.csv`

echo "the count is $e"

if [ "$e" -gt 0 ]
then
    echo  "the no of shipments with  pickup_reattempt count > 5 is $e"
 else
      echo  "No shipments having pickup_reattempt count 5"
fi

echo "$query1"|mysql -N  -u$decoded_shipping_ro_user -p$decoded_shipping_ro_pass -h$shipping_ro_ip $decoded_shipping_db_name >> $cwd/five_days.csv

f=`wc -l <$cwd/MYS_five_days.csv`

echo "the count is $f"

if [ "$f" -gt 0 ]
then
    echo  "the no of MYS shipments with  pickup_reattempt count > 5 is $f"
 else
      echo  "No MYS shipments have pickup_reattempt count 5"
fi

echo "$squery"|mysql -N  -u$decoded_shipping_ro_user -p$decoded_shipping_ro_pass -h$shipping_ro_ip $decoded_shipping_db_name >> $cwd/ten_days.csv

d=`wc -l <$cwd/ten_days.csv`

echo "the count is $d"


if [ "$d" -gt 0 ]
then
   echo  "the no of shipments with  pickup_reattempt count > 10 is $d"
 else
      echo  "No shipments having pickup_reattempt count 10"
fi

echo "merging both the files"

cat $cwd/five_days.csv $cwd/MYS_five_days.csv $cwd/ten_days.csv  >> $cwd/ext_rtoo.csv

sort $cwd/ext_rtoo.csv | uniq > $cwd/ext_rto.csv

r=`wc -l <$cwd/ext_rto.csv`

echo "the count is $r"

if [ "$r" -gt 0 ]
then
   ( echo -e   "Hi Team,\nThe number of shipments eligible for RTO is $r" ;uuencode $cwd/ext_rto.csv $cwd/ext_rto.csv  ) | mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO
 else
      echo  "No shipments are eligible for RTO"| mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO
fi

echo "RTO marking started"

while read p; do
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"merchant_reference_id":"$p","reason":"attempts_exhausted"}' http://10.24.1.19/shipments/$p/rto
done<$cwd/ext_rto.csv

while read -r line; do
      tracking_id=$(echo $line | cut -d ' ' -f 1)
      merchant_reference_id=$(echo $line | cut -d ' ' -f 2)
      data_object=$(jq -n --arg tracking_id "$tracking_id" --arg merchant_reference_id "$merchant_reference_id" '{"request_details": [ {tracking_id: $tracking_id, merchant_reference_id: $merchant_reference_id, reason: "Pickup reattempts exceeded"}]}')
      echo $data_object > data.json
      curl --location --request PUT 'https://api.ekartlogistics.com/v2/shipments/rto/create' --header 'HTTP_X_MERCHANT_CODE: MYS' --header 'Content-Type: application/json' --header 'X_CALLBACK_CODES: MYS' --header 'X_RESTBUS_DESTINATION_RESPONSE_STATUS: 200' --header 'Authorization: Basic ZmxpcGthcnQ6RyRLNClXUjNmKERxZUdyZw==' --data @data.json
done <<(cat MYS_five_days.csv)

rm $cwd/ext_*.csv $cwd/five_days.csv $cwd/MYS_five_days.csv $cwd/ten_days.csv $cwd/fif_days.csv

echo "RTO Marking done. tada !!"
