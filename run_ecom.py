# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

## sessions
start_date = '2019-10-20'
end_date = '2019-10-20'

# base dims and all metrics
ecom1 = return_ga_data(
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

        # event parameters
        {'name': 'ga:eventAction'},
        {'name': 'ga:eventLabel'},

        # custom dimensions
        {'name': 'ga:dimension12'}, # guests
        {'name': 'ga:dimension13'} # context
    ],
    group_by = ['ga:dimension1', 'ga:dimension3', 'ga:productName', 'ga:eventAction', 'ga:eventLabel',  'ga:dimension12', 'ga:dimension13'],
    dimensionFilterClauses = [],
    segments=[]
)


ecom2 = return_ga_data(
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

        # event parameters
        {'name': 'ga:eventAction'},
        {'name': 'ga:eventLabel'},

        # custom dimensions
        {'name': 'ga:dimension15'}, # context
    ],
    group_by = ['ga:dimension1', 'ga:dimension3', 'ga:productName', 'ga:eventAction', 'ga:eventLabel',  'ga:dimension15'],
    dimensionFilterClauses = [],
    segments=[]
)

# join
## for sampling flag use result from events1. Same time frame so result will be the same regardless
ecom_combined = ecom1.merge(ecom2.drop(['ga:itemRevenue', 'ga:itemQuantity', 'ga:productListViews', 'ga:productDetailViews', 'ga:productAddsToCart', 'ga:productRemovesFromCart', 'ga:productCheckouts', 'ga:uniquePurchases', 'sampling',], axis = 1),
                                   on = ['ga:dimension1', 'ga:dimension3', 'ga:productName', 'ga:eventAction', 'ga:eventLabel'],
                                   how = 'left')

# in case created with left join
ecom_combined.fillna('na', inplace = True)

ecom_ordered = [

    # dimensions
    'ga:dimension1',
    'ga:dimension3',
    'ga:eventAction',
    'ga:eventLabel',
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

ecom_combined = ecom_combined[ecom_ordered]

ecom_nice_names = [

    # dimensions
    'dimension1',
    'dimension3',
    'event_action',
    'event_label',
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
ecom_combined.columns = ecom_nice_names

# Clear memory. Might not be necessary but still haunted from using R
del ecom_nice_names, ecom1, ecom2

# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
ecom_combined.to_sql('ecom',
             con = engine,
             schema = 'ga_photo_booker',
             index = False,
             if_exists = 'append')