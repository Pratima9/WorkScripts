#!/bin/sh
set -x
source /usr/share/3pl-crons/Externalization_Scripts/client_facing_reports/db_functions.sh
. /usr/share/3pl-crons/db/db_config.sh

$MAILFROM="pratima.u@flipkart.com"
$SUBJECT="Shipping status"
$MAILTO="pratima.u@flipkart.com"

CWD='/home/pratima.u/'

split -l 10000 new.csv split_tracking_ids

n=1

for f in split_tracking_ids*
do
  SPLIT_TRACKING_IDS=`awk -F, '{print "'\''" $1 "'\''"}' $f | paste -sd,`
  shipping_weight_query="select vendor_tracking_id , state from shipments  where  vendor_tracking_id in (${SPLIT_TRACKING_IDS})";
  fkl_slave "${shipping_weight_query}" | sed 's|\t|,|g' >> $CWD/shipping_state_$n.csv
  ((n++))
  echo "Batch $n done"
done

echo "tracking_id , state"  >> $CWD/headers.csv

FILENAME=$CWD/shipping_state_$n.csv

count=`wc -l $CWD/shipping_state_$n.csv | awk '{print $1}'`
line_count="$(($count - 1))"
echo "File Line Count: $line_count"

if [ $line_count -lt 200000 ]
then
gzip $CWD/$FILENAME
uuencode $CWD/$FILENAME.gz $CWD/$FILENAME.gz|mailx -a "From: $MAILFROM" -s $SUBJECT $MAILTO
else
sed '1d' $CWD/$FILENAME > $CWD/temp_$FILENAME
                         split -l 200000 $CWD/temp_$FILENAME $CWD/${MERCHANT_CODE}_temp_MIS_file_
                         i=1
                         for fl in `ls -1 $CWD|grep "temp_MIS_file_"|xargs`
                              do
                              echo "$CWD/$fl"
  cat $CWD/headers.csv $CWD/$fl |sed 's/NULL/N\/A/g' > $CWD/${FILENAME}_${i}.csv
                              gzip $CWD/${FILENAME}_${i}.csv
                              uuencode $CWD/${FILENAME}_${i}.csv.gz $CWD/${FILENAME}_${i}.csv.gz|mailx -a "From: $MAILFROM" pratima.u@flipkart.com -s "$SUBJECT" Shipping status $MAILTO pratima.u@flipkart.com
                              i=$((i+1))
                            done
fi

rm *xa* *xb* *xc* *xd* rm *split_tracking_id*
