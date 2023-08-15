#!/bin/bash

set -x

decoded_shipping_ro_user=`cat /etc/db_config.json|jq .shipping[].SlaveUser|sed 's/"//g'`
encrypted_shipping_ro_pass=`cat /etc/db_config.json|jq .shipping[].SlavePassword|sed 's/"//g'`
shipping_ro_ip=`cat /etc/db_config.json|jq .shipping[].hosts[].slave4|sed 's/"//g'`
decoded_shipping_db_name=`cat /etc/db_config.json|jq .shipping[].Database|sed 's/"//g'`
decoded_shipping_ro_pass=`bash /usr/share/3pl-crons/db/get_db_cred.sh  3pl-crons-readuser $encrypted_shipping_ro_pass`
CRYPTEX_READ_ONLY_CLIENT_ID=`cat /etc/db_config.json|jq .cryptex[].read_only_client_id|sed 's/"//g'`
CRYPTEX_READ_ONLY_CLIENT_SECRET=`cat /etc/db_config.json|jq .cryptex[].read_only_client_secret|sed 's/"//g'`
FSDSSH_RO_USER="erp-fklogis_ro"
FSDSSH_RO_HOST="prod-hyd.ekl-erp-fklogistics-db.dashboard-fsd-analytics-slave.corelogistics-db.fkcloud.in"
FSDSSH_DB_NAME="fklogistics"
FSDSSH_RO_PASSWORD="Veey4nu1"

CWD='/mnt/archive/CL_diff'
DATE=`date +"%F" `
CURR_DATE=`date +"%Y-%m-%d %H:%M:%S"`
minus_one_hour=$(date -d "$CURR_DATE" -d "1 hour ago" +"%Y-%m-%d %H:%M:%S")
minus_two_hour=$(date -d "$CURR_DATE" -d "2 hour ago" +"%Y-%m-%d %H:%M:%S")
LAST_DATE=`date +\%Y-\%m-\%d -d "-1 days"`

export MAILFROM="Shipping-dev  <logistics-orch-pse-team@flipkart.com>"
export MAILTO="pratima.u@flipkart.com,preeteesh.sharma@flipkart.com,mridul.mundara@flipkart.com"
export SUBJECT="Shipments diff| ${CURR_DATE} "


query="select vendor_tracking_id from shipments where vendor_id in (12,38) and created_at >='2023-06-09 21:00:00'"

echo "$query"|mysql -N  -u$decoded_shipping_ro_user -p$decoded_shipping_ro_pass -h$shipping_ro_ip $decoded_shipping_db_name >> $CWD/fetch_myntra_temp.csv

awk '!seen[$0]++' $CWD/fetch_myntra_temp.csv >> $CWD/fetch_myntra.csv

split -l 5000 $CWD/fetch_myntra.csv $CWD/dataCL

for f in $CWD/dataCL*
do
        SPLIT_TRACKING_IDS=`awk -F, '{print "'\''" $1 "'\''"}' $f | paste -sd,`
        squery="SELECT sre.shipmentId FROM shipmentRouteEvent sre WHERE shipmentId IN (${SPLIT_TRACKING_IDS});"
        echo "$squery" |mysql -N  -u$FSDSSH_RO_USER -p$FSDSSH_RO_PASSWORD -h$FSDSSH_RO_HOST $FSDSSH_DB_NAME | uniq | sed 's|\t|,|g' >> $CWD/CL_final_temp.csv

        ((n++))
        echo "Batch $n done"
done

rm $CWD/dataCL* $CWD/fetch_myntra_temp.csv

awk '!seen[$0]++' $CWD/CL_final_temp.csv >> $CWD/CL_final.csv

sort $CWD/fetch_myntra.csv $CWD/CL_final.csv | uniq -u > $CWD/diff_CL.csv

#awk -F',' 'NR==FNR{a[$0];next} !($0 in a)' $CWD/fetch_myntra.csv $CWD/CL_final.csv > $CWD/diff_CL.csv  #finding diff

gzip $CWD/diff_CL.csv

(echo -e " Hi People,\n Pls find the diff between shipping and CL";uuencode $CWD/diff_CL.csv.gz $CWD/diff_CL.csv.gz) |mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO



echo "querying done and mailed tada!!!"
