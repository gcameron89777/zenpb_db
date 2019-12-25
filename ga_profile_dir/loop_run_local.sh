
startdate=2019-12-01
enddate=2019-12-23
sDateTs=$(date -j -f "%Y-%m-%d" $startdate "+%s")
eDateTs=$(date -j -f "%Y-%m-%d" $enddate "+%s")
dateTs=$sDateTs
offset=86400 # seconds in a  day

while [[ "$dateTs" -le "$eDateTs" ]]
do
  d=$(date -j -f "%s" $dateTs "+%Y-%m-%d")
  echo 'Starting Data Extract ' $d
  echo 'Starting Session Extract ' $d
  python3.7 flagship_ecom/run_sessions.py $d
  echo 'Starting Page Views Extract ' $d
  python3.7 flagship_ecom/run_pageviews.py $d
  echo 'Starting Events Extract ' $d
  python3.7 flagship_ecom/run_events.py $d
  echo 'Starting ECOM Extract ' $d
  python3.7 flagship_ecom/run_ecom.py $d
  echo 'Starting Transactions Extract ' $d
  python3.7 flagship_ecom/run_transactions.py $d
  echo 'Extract completed!'
  dateTs=$(($dateTs+$offset))
done
