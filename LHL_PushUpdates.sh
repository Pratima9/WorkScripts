#!/bin/bash

set -x

. /usr/share/3pl-crons/db/db_config.sh

CURR_DATE=`date '+%Y%m%d'`
CWD='/home/pratima.u'

export MAILFROM="ext-oncall  <ekart-ext-oncall@flipkart.com>"
export MAILTO="pratima.u@flipkart.com,pranav.tawate@pwc.com,nikhil.patil@lighthouse-learning.com,ashish.ketkar@lighthouse-learning.com,divakar.karanth@lighthouse-learning.com,jyothi.gopinathan@flipkart.com,pratik.patil@lighthouse-learning.com,sreeragh.s@flipkart.com,vivekanand.mishra@flipkart.com,mridul.mundara@flipkart.com,bikrant.sahoo@flipkart.com"
export SUBJECT="LHL Push updates report | ${CURR_DATE}"

# query="SELECT CONCAT(group_id, '\t', message) FROM outbound_messages_client_status_update_d_$CURR_DATE WHERE group_id LIKE 'LHL%'";

# echo "$query"|mysql -N  -u${EKL_DURIN_USER} -p${EKL_DURIN_PWD} -h${EKL_DURIN_HOST} ${EKL_DURIN_DB} > $CWD/durin_result.csv

query="SELECT CONCAT(group_id, ',', message) FROM outbound_messages_client_status_update_d_$CURR_DATE WHERE group_id LIKE 'LHL%'"
mysql -N -u${EKL_DURIN_USER} -p${EKL_DURIN_PWD} -h${EKL_DURIN_HOST} ${EKL_DURIN_DB} -B -e "$query" > $CWD/durin_result.csv

# parse_json_data() {
#     local json_data="$1"
#     local second_column_value
#     second_column_value=$(echo "$json_data" | jq -r '.second_column')
#     echo "Parsed value from the second column: $second_column_value"
# }

echo -e "Tracking Id","Payload" >> LHL_data.csv
while IFS=',' read -r line; do
    if echo "$line" | grep -qE "pickup_done|pickup_cancelled|shipment_dispatched|shipment_delivered"; then
        # Print the entire line
        echo "$line" >> $CWD/LHL_data.csv
    fi
done < "$CWD/durin_result.csv"
awk '!/^Tracking ID,Payload$/' $CWD/durin_result.csv > $CWD/output.csv && mv $CWD/output.csv $durin_result.csv

(echo -e "Hey User,\nPls find the LHL push updates for events pickup_done, pickup_cancelled, shipment_dispatched, shipment_delivered"; uuencode $CWD/LHL_data.csv LHL_Data_$CURR_DATE.csv ; uuencode $CWD/durin_result.csv LHL_DataAll_$CURR_DATE.csv) | mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO

rm $CWD/durin_result.csv $CWD/LHL_data.csv
