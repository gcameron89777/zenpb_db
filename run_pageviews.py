# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

## sessions
start_date = '2019-09-04'
end_date = '2019-10-19'

# base dims and all metrics
pages = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:pageviews'},
    {'expression': 'ga:uniquePageviews'},
    {'expression': 'ga:timeOnPage'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # page dimensions
    {'name': 'ga:hostname'},
    {'name': 'ga:pagePath'}

  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:hostname', 'ga:pagePath'],
  dimensionFilterClauses = [],
  segments=[]
)

pages.fillna('na', inplace = True)

pages_ordered = [
    'ga:dimension1',
    'ga:dimension3',
    'ga:hostname',
    'ga:pagePath',
    'ga:pageviews',
    'ga:uniquePageviews',
    'ga:timeOnPage',
    'sampling'
]

pages = pages[pages_ordered]

pages_nice_names = ['dimension1',
                    'dimension3',
                    'host_name',
                    'page_path',
                    'page_views',
                    'unique_page_views',
                    'time_on_page',
                    'sampling']

pages.columns = pages_nice_names

# Clear memory. Might not be necessary but still haunted from using R
del pages_nice_names

# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
pages.to_sql('pageviews',
             con = engine,
             schema = 'ga_photo_booker',
             index = False,
             if_exists = 'append')

