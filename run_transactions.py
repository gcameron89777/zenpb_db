# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

## sessions
start_date = '2019-09-01'
end_date = '2019-10-19'

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
  dimensionFilterClauses = [],
  segments=[]
)

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

# Clear memory. Might not be necessary but still haunted from using R
del trans_nice_names, trans_ordered

# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
transactions.to_sql('transactions',
             con = engine,
             schema = 'ga_photo_booker',
             index = False,
             if_exists = 'append')

