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

# for console logs
print('start preprocessing pageviews data')

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

# where timezone offset out of bounds, remove it
#pages['dimension3'] = pages['dimension3'].apply(lambda x: rogue_tz_offsets(x))

# parallel processing with swifter
pages['dimension3'] = pages['dimension3'].swifter.apply(lambda x: rogue_tz_offsets(x))

# Clear memory. Might not be necessary but still haunted from using R
del pages_nice_names


print('start uploading pageviews data to Azure')
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
pages.to_sql('pageviews',
             con = engine,
             schema = 'ga_flagship_ecom',
             index = False,
             if_exists = 'append')