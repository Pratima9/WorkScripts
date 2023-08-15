#!/bin/bash

set -x

host=10.24.40.139
user="srms_ro"
password="4j^WbaSmd1"
port=4000
database="srms"


CWD='/mnt/archive/'
DATE=`date +"%F" `
CURR_DATE=`date +"%b-%d"`
YEST_DATE=`date +\%Y-\%m-\%d -d "-1 days"`
LAST_DATE=`date +\%Y-\%m-\%d -d "-7 days"`

export MAILFROM="Shipping-Dev  <pratima.u@flipkart.com>"
export MAILTO="pratima.u@flipkart.com"
export SUBJECT="SRMS Shipment States| ${CURR_DATE}"
input_file_name=$CWD/srms_states.csv

comma_separated_file_input="commaseparated_data_$DATE"

#squery="select distinct count(*) as Count,client_id as ClientCode from sr where status='SentToShipping' and DATE(created_at) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY) group by client_id";

echo "Connecting to SRMS shards"

#query_result=echo "$squery" |mysql -N  -u$user -p$password -h$host -P$port $database | uniq | sed 's|\t|,|g'
query_result=$(mysql -N -u$user -p$password -h$host -P$port $database -e "select distinct count(*) as Count,client_id as ClientCode from sr where status='SentToShipping' and DATE(created_at) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY) group by client_id")

print_table() {
    printf "+-------+--------------+\n"
    printf "| %-5s | %-12s |\n" "Count" "Client Code"
    printf "+-------+--------------+\n"
    echo -e "$1" | while read -r Count ClientCode; do
        printf "| %-5s | %-12s |\n" "$Count" "$ClientCode"
    done
    printf "+-------+--------------+\n"
}

table=$(print_table "$query_result")

echo -e  "Hi,\n The number of shipments stuck in srms in SentToShipping state from $LAST_DATE to $YEST_DATE is \n$table" |mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO

#rm $CWD/srms_states.csv
