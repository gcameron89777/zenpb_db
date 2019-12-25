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
ecom = return_ga_data(
    start_date = start_date,
    end_date = end_date,
    view_id = VIEW_ID,
    metrics = [
        {'expression': 'ga:itemRevenue'},
        {'expression': 'ga:itemQuantity'},
        {'expression': 'ga:productListViews'},
        {'expression': 'ga:productDetailViews'},
        {'expression': 'ga:productAddsToCart'},
        {'expression': 'ga:productRemovesFromCart'},
        {'expression': 'ga:productCheckouts'},
        {'expression': 'ga:uniquePurchases'},
    ],
    dimensions = [
        {'name': 'ga:dimension1'}, # session ID
        {'name': 'ga:dimension3'}, # timestamp
        {'name': 'ga:productName'},

        # custom dimensions
        {'name': 'ga:dimension11'}, #bundle
        {'name': 'ga:dimension15'}, # shipping method
        {'name': 'ga:dimension17'}, # fullfillment type
        {'name': 'ga:dimension18'} # add to cart context

    ],
    group_by = ['ga:dimension1', 'ga:dimension3', 'ga:productName', 'ga:dimension11',
                'ga:dimension15', 'ga:dimension17', 'ga:dimension18'],
    dimensionFilterClauses = [],
    segments=[]
)


print('start pro processing ecom data')
ecom_ordered = [

    # dimensions
    'ga:dimension1',
    'ga:dimension3',
    'ga:productName',
    'ga:dimension11',
    'ga:dimension15',
    'ga:dimension17',
    'ga:dimension18',

    # metrics
    'ga:productListViews',
    'ga:productDetailViews',
    'ga:productAddsToCart',
    'ga:productRemovesFromCart',
    'ga:productCheckouts',
    'ga:uniquePurchases',
    'ga:itemQuantity',
    'ga:itemRevenue',
    'sampling'
]

ecom = ecom[ecom_ordered]

ecom_nice_names = [

    # dimensions
    'dimension1',
    'dimension3',
    'product_name',
    'dimension11',
    'dimension15',
    'dimension17',
    'dimension18',

    # metrics
    'product_list_views',
    'product_detail_views',
    'product_adds_to_cart',
    'product_removes_from_cart',
    'product_checkouts',
    'unique_purchases',
    'item_quantity',
    'item_revenue',
    'sampling']

ecom.columns = ecom_nice_names

# where timezone offset out of bounds, remove it
ecom['dimension3'] = ecom['dimension3'].swifter.apply(lambda x: rogue_tz_offsets(x))

print('start uploading ecom data to Azure')
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
ecom.to_sql('ecom',
            con = engine,
            schema = 'ga_flagship_ecom',
            index = False,
            if_exists = 'append')