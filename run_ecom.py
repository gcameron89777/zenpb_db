# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

start_date = creds.start_date
end_date = creds.end_date

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
        {'name': 'ga:dimension12'}, # guests
        {'name': 'ga:dimension13'}, # photographer attributes
        {'name': 'ga:dimension15'} # context
    ],
    group_by = ['ga:dimension1', 'ga:dimension3', 'ga:productName', 'ga:dimension12', 'ga:dimension13', 'ga:dimension15'],
    dimensionFilterClauses = [],
    segments=[]
)


ecom_ordered = [

    # dimensions
    'ga:dimension1',
    'ga:dimension3',
    'ga:productName',
    'ga:dimension12',
    'ga:dimension13',
    'ga:dimension15',

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
    'dimension12',
    'dimension13',
    'dimension15',

    # metrics
    'product_list_views',
    'product_detail_views',
    'product_adds_to_cart',
    'product_removes_from_cart',
    'product_checkouts',
    'unique_purchases',
    'item_quantity',
    'item_revenue',
    'sampling'

]
ecom.columns = ecom_nice_names


# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
ecom.to_sql('ecom',
            con = engine,
            schema = 'ga_photo_booker',
            index = False,
            if_exists = 'append')