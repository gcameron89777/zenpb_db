# packages and globals
# importing from another directory, add dir with sys path
import sys
sys.path.append('.')
from functions import return_ga_data, rogue_tz_offsets
import credentials as creds
import runtime as run
import psycopg2
from sqlalchemy import create_engine
import swifter


VIEW_ID = run.flag_ecom_viewid
start_date = run.start_date
end_date = run.end_date

# run in loop dates
# start_date = sys.argv[1]
# end_date = start_date

# base dims and all metrics
transactions = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:transactionRevenue'},
    {'expression': 'ga:transactionTax'},
    {'expression': 'ga:transactionShipping'},
    {'expression': 'ga:itemQuantity'},
    {'expression': 'ga:totalRefunds'},
    {'expression': 'ga:refundAmount'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp
    {'name': 'ga:dimension4'}, # account id
    {'name': 'ga:transactionId'}
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:dimension4', 'ga:transactionId'],
  dimensionFilterClauses=[],
  segments=[]
)

import sys
if transactions.shape[0] == 0:
    sys.exit(0)

transactions.fillna('na', inplace = True)

trans_ordered = [

    # dimensions
    'ga:dimension1',
    'ga:dimension3',
    'ga:dimension4',
    'ga:transactionId',

    # metrics
    'ga:transactionRevenue',
    'ga:transactionTax',
    'ga:transactionShipping',
    'ga:itemQuantity',
    'ga:totalRefunds',
    'ga:refundAmount',
    'sampling'
]

transactions = transactions[trans_ordered]

trans_nice_names = ['dimension1',
                    'dimension3',
                    'dimension4',
                    'transaction_id',
                    'transaction_revenue',
                    'transaction_tax',
                    'transaction_shipping',
                    'item_quantity',
                    'total_refunds',
                    'refund_amount',
                    'sampling']

transactions.columns = trans_nice_names

# where timezone offset out of bounds, remove it
transactions['dimension3'] = transactions['dimension3'].swifter.apply(lambda x: rogue_tz_offsets(x))

# Clear memory. Might not be necessary but still haunted from using R
del trans_nice_names, trans_ordered

# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       executemany_mode='values',
                       executemany_values_page_size=10000,
                       echo=False)

# post to sessions table
transactions.to_sql('transactions',
             con = engine,
             schema = 'ga_flagship_ecom',
             index = False,
             if_exists = 'append')

