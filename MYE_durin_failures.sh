#!/bin/bash
#daily reports for creation failures
#By Pratima.u
. /usr/share/3pl-crons/db/db_config.sh

echo ${EKL_DURIN_DB}
CWD='/mnt/archive/MYS_duin_failures'
Yesterday=`date -d "${dtd} -1 days" +'%Y-%m-%d'`
if [[ $# -eq 1 ]] ; then
	Yesterday=$1
fi
YEST_DATE_YYYYMMDD=`echo "$Yesterday" | tr -d -`

function durin_slave() {
      echo "$1" | mysql -u${EKL_DURIN_USER} -p${EKL_DURIN_PWD} -h${EKL_DURIN_HOST} ${EKL_DURIN_DB}  -N
      }

    TID_FAILURES="select vendorTrackingId from creation_failures_d_${YEST_DATE_YYYYMMDD}"

    durin_slave "${TID_FAILURES}" | sort | uniq | sed 's|\t|,|g' > $CWD/durin_fail_33${YEST_DATE_YYYYMMDD}.csv

    awk '{printf("'"'"'%s'"'"',", $0)} END{print ""}' $CWD/durin_fail_33${YEST_DATE_YYYYMMDD}.csv |sed -e 's/^/'\('/g' |sed -e 's/$/'\)'/g' |sed 's/\,)/\)/' > $CWD/durin_fail_34${YEST_DATE_YYYYMMDD}.csv

    DURIN_STALE_FAILURES_TIDS="select vendorTrackingId from clientIdMapping where vendorTrackingId in $(cat $CWD/durin_fail_34$YEST_DATE_YYYYMMDD.csv)"

    durin_slave "${DURIN_STALE_FAILURES_TIDS}" | sort | uniq | sed 's|\t|,|g' > $CWD/durin_pass_21${YEST_DATE_YYYYMMDD}.csv

    if [ $(wc -l <$CWD/durin_pass_21${YEST_DATE_YYYYMMDD}.csv) -ne 0 ]
    then

        awk '{printf("'"'"'%s'"'"',", $0)} END{print ""}' $CWD/durin_pass_21${YEST_DATE_YYYYMMDD}.csv |sed -e 's/^/'\('/g' |sed -e 's/$/'\)'/g' |sed 's/\,)/\)/' > $CWD/durin_pass_22${YEST_DATE_YYYYMMDD}.csv

        DURIN_REAL="select vendorTrackingId from creation_failures_d_$YEST_DATE_YYYYMMDD where vendorTrackingId not in $(cat $CWD/durin_pass_22$YEST_DATE_YYYYMMDD.csv)"
        #awk 'FNR == NR {T[$1]; next} {for (t in T) sub (t, _)} 1' DURIN_STALE_FAILURES_TIDS TID_FAILURES > REAL${YEST_DATE_YYYYMMDD}.csv
        durin_slave "${DURIN_REAL}" | sort | uniq | sed 's|\t|,|g' > $CWD/durin_fail_26${YEST_DATE_YYYYMMDD}.csv

        awk '{printf("'"'"'%s'"'"',", $0)} END{print ""}' $CWD/durin_fail_26${YEST_DATE_YYYYMMDD}.csv|sed -e 's/^/'\('/g' |sed -e 's/$/'\)'/g' |sed 's/\,)/\)/' > $CWD/durin_fail_27${YEST_DATE_YYYYMMDD}.csv

        FAILURE_DURIN_SHIPMENTS="select cf.vendorTrackingId,\"serviceability failure\" as Reason,cf.reason as Description,cf.createdDateTime as Event_Date from creation_failures_d_${YEST_DATE_YYYYMMDD} cf inner join (select vendorTrackingId, max(id) as max_id from creation_failures_d_${YEST_DATE_YYYYMMDD} group by vendorTrackingId) max_id_tbl on (cf.vendorTrackingId=max_id_tbl.vendorTrackingId and cf.id=max_id_tbl.max_id) and cf.vendorTrackingId in $(cat $CWD/durin_fail_27$YEST_DATE_YYYYMMDD.csv)";

        durin_slave "${FAILURE_DURIN_SHIPMENTS}" | uniq | sed 's|\t|,|g' > $CWD/durin_failure_shipments_part1_${YEST_DATE_YYYYMMDD}.csv
    else
        FAILURE_DURIN_SHIPMENTS_ELSE="select cf.vendorTrackingId,\"serviceability failure\" as Reason,cf.reason as Description,cf.createdDateTime as Event_Date from creation_failures_d_${YEST_DATE_YYYYMMDD} cf inner join (select vendorTrackingId, max(id) as max_id from creation_failures_d_${YEST_DATE_YYYYMMDD} group by vendorTrackingId) max_id_tbl on (cf.vendorTrackingId=max_id_tbl.vendorTrackingId and cf.id=max_id_tbl.max_id) and cf.vendorTrackingId in $(cat $CWD/durin_fail_34$YEST_DATE_YYYYMMDD.csv)";
        durin_slave "${FAILURE_DURIN_SHIPMENTS_ELSE}" | uniq | sed 's|\t|,|g' > $CWD/durin_failure_shipments_part1_${YEST_DATE_YYYYMMDD}.csv
    fi

    DURIN_ERROR_TIDS=`gawk -F, '{print "\""$1"\""}' $CWD/durin_failure_shipments_part1_${YEST_DATE_YYYYMMDD}.csv | paste -sd,`

    GET_CLIENT_REQUESTS="select cf.clientRequest from creation_failures_d_${YEST_DATE_YYYYMMDD} cf inner join (select vendorTrackingId, max(id) as max_id from creation_failures_d_${YEST_DATE_YYYYMMDD} group by vendorTrackingId ) max_id_tbl on (cf.vendorTrackingId=max_id_tbl.vendorTrackingId and cf.id=max_id_tbl.max_id ) where cf.vendorTrackingId in (${DURIN_ERROR_TIDS})";

    durin_slave "${GET_CLIENT_REQUESTS}"  > $CWD/durin_creation_failures_requests_${YEST_DATE_YYYYMMDD}.txt
    rm $CWD/durin_failure_shipments_part2_${YEST_DATE_YYYYMMDD}.csv
    touch $CWD/durin_failure_shipments_part2_${YEST_DATE_YYYYMMDD}.csv

while read message;
do
    client_name=`echo $message|jq -r ".client_name"`
    if [ $client_name == 'MYE' ]
    then
        service_code=`echo $message|jq -r ".services[].service_code"`
        service_leg=`echo $message|jq -r ".services[].service_details[].service_leg"`
        tracking_id=`echo $message|jq -r ".services[].service_details[].shipment.tracking_id"`
        client_reference_id=`echo $message|jq -r ".services[].service_details[].shipment.client_reference_id"`
        fulfillment_type=`echo $message|jq -r ".services[].service_details[].service_data.fulfillment_type"`
        if [ $fulfillment_type == "fbf" ]
        then
            source_location_code=`echo $message|jq -r ".services[].service_details[].service_data.source.location_code"`
            return_location_code=`echo $message|jq -r ".services[].service_details[].service_data.return_location.location_code"`
        else
            source_location_code=`echo $message|jq -r ".services[].service_details[].service_data.source.seller_location_id"`
            return_location_code=`echo $message|jq -r ".services[].service_details[].service_data.return_location.seller_location_id"`
        fi
        source_pincode=`echo $message|jq -r ".services[].service_details[].service_data.source.address.pincode"`

        if [ $service_leg == "FORWARD" ]
        then
            destination_pincode=`echo $message|jq -r ".services[].service_details[].service_data.destination.address.pincode"`
        else
            destination_pincode=`echo $message|jq -r ".services[].service_details[].service_data.destination.location_code"`
        fi
        return_pincode=`echo $message|jq -r ".services[].service_details[].service_data.return_location.address.pincode"`

        echo $tracking_id,$service_code,$service_leg,$client_reference_id,$source_location_code,$source_pincode,$destination_pincode,$return_location_code,$return_pincode >> $CWD/durin_failure_shipments_part2_${YEST_DATE_YYYYMMDD}.csv
    fi
done < $CWD/durin_creation_failures_requests_${YEST_DATE_YYYYMMDD}.txt


		echo "TrackingId,Reason,Description,Event_Date,ShipmentType,Service_leg,ClientReferenceId,Source_location_code,Source_Pincode,Destination_Pincode,Return_location_code,Return_pincode" > ${YEST_DATE_YYYYMMDD}_durin_failure_shipments.csv

        join -1 1 -2 1 -t, -o 1.1,1.2,1.3,1.4,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9 <(sort -t , -k1 $CWD/durin_failure_shipments_part1_${YEST_DATE_YYYYMMDD}.csv) <(sort -t , -k1 $CWD/durin_failure_shipments_part2_${YEST_DATE_YYYYMMDD}.csv ) >> ${YEST_DATE_YYYYMMDD}_durin_failure_shipments.csv




FILENAME=${YEST_DATE_YYYYMMDD}_durin_failure_shipments.csv
export MAILFROM="Myntra Synergy<ekart-ext-oncall@flipkart.com>"
#export MAILTO="pratima.u@flipkart.com"
export MAILTO="nucleus@flipkart.com,pratima.u@flipkart.com,koushik.rout@flipkart.com,raviteja.jagirdar@flipkart.com,arika.roy@myntra.com,gurudutt.s@myntra.com,apoorva.mittal@myntra.com,mrunmay.dash@flipkart.com,mayuri.k@myntra.com,subhanshu.pareek@flipkart.com,balaji.n@flipkart.com,gaurav.bansal@flipkart.com,ajayk.r@flipkart.com,pandey.alok@flipkart.com,jigar.shah@flipkart.com"
export SUBJECT="MYE shipment creation failures in durin on ${Yesterday}"
export ATTACH="${FILENAME}"
export MAIL_FAILED_ID="ekart-externalization-oncall@flipkart.com"


if [ `wc -l $FILENAME | awk '{print $1}' ` -lt 2 ]
then
echo ""| mail -s "Daily Report for creation failures failed !!!" ${MAIL_FAILED_ID}
else
(
echo -e   "Hi Team, \nplease find attached MYE creation failures." ;uuencode $ATTACH $(basename $ATTACH)
) | mailx -a "From: $MAILFROM" -s "$SUBJECT" $MAILTO
#/usr/sbin/sendmail -t
fi

echo "MAIL SENT"

rm -rf "${FILENAME}" $CWD/durin_failure_shipments_part1_${YEST_DATE_YYYYMMDD}.csv $CWD/durin_pass_21${YEST_DATE_YYYYMMDD}.csv $CWD/durin_pass_22${YEST_DATE_YYYYMMDD}.csv $CWD/durin_failure_shipments_part2_${YEST_DATE_YYYYMMDD}.csv $CWD/durin_fail_26${YEST_DATE_YYYYMMDD}.csv $CWD/durin_fail_27${YEST_DATE_YYYYMMDD}.csv $CWD/durin_fail_33${YEST_DATE_YYYYMMDD}.csv $CWD/durin_fail_34${YEST_DATE_YYYYMMDD}.csv $CWD/durin_creation_failures_requests_${YEST_DATE_YYYYMMDD}.txt
