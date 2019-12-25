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
events1 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:totalEvents'},
    {'expression': 'ga:uniqueEvents'},
    {'expression': 'ga:eventValue'},
    {'expression': 'ga:goal1completions'},
    {'expression': 'ga:goal2completions'},
    {'expression': 'ga:transactions'},
    {'expression': 'ga:transactionRevenue'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # event parameters
    {'name': 'ga:eventCategory'},
    {'name': 'ga:eventAction'},
    {'name': 'ga:eventLabel'},
    {'name': 'ga:dimension4'}, # account id
    {'name': 'ga:dimension5'} # plan

  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventCategory', 'ga:eventAction',
              'ga:eventLabel', 'ga:dimension4', 'ga:dimension5'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with custom dimensions
events2 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:totalEvents'} # will drop afterwards since already have in events1, only need a metric to hit the api with
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension6'}, # type
    {'name': 'ga:dimension7'},  # theme
    {'name': 'ga:dimension8'},  # account name
    {'name': 'ga:dimension9'},  # getting started progress
    {'name': 'ga:dimension10'}  # layout
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:dimension6',
              'ga:dimension7', 'ga:dimension8', 'ga:dimension9',
              'ga:dimension10'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with custom dimensions
events3 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:totalEvents'} # will drop afterwards since already have in events1, only need a metric to hit the api with
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension12'}, # photo organiser
    {'name': 'ga:dimension13'},  # website customize
    {'name': 'ga:dimension14'}  # selling
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:dimension12',
              'ga:dimension13', 'ga:dimension14'],
  dimensionFilterClauses = [],
  segments=[]
)


print('Start pre processing events data')
# join
## for sampling flag use result from events1. Same time frame so result will be the same regardless


events_combined = events1.merge(events2.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                on = ['ga:dimension1', 'ga:dimension3'],
                                how = 'left').merge(events3.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                                    on = ['ga:dimension1', 'ga:dimension3'],
                                                    how = 'left')

# some leading or trailing whitespace on some fields causing dups
# remove leading or trailing whitespace then dedup
events_combined = events_combined.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
events_combined.fillna('na', inplace = True)
events_combined = events_combined.drop_duplicates()

## Get fields in order
events_ordered = ['ga:dimension1',
                  'ga:dimension3',
                  'ga:eventCategory',
                  'ga:eventAction',
                  'ga:eventLabel',
                  'ga:dimension4',
                  'ga:dimension5',
                  'ga:dimension6',
                  'ga:dimension7',
                  'ga:dimension8',
                  'ga:dimension9',
                  'ga:dimension10',
                  'ga:dimension12',
                  'ga:dimension13',
                  'ga:dimension14',
                  'ga:totalEvents',
                  'ga:uniqueEvents',
                  'ga:eventValue',
                  'ga:goal1completions',
                  'ga:goal2completions',
                  'ga:transactions',
                  'ga:transactionRevenue',
                  'sampling']

events_combined = events_combined[events_ordered]

event_nice_names = ['dimension1',
                    'dimension3',
                    'event_category',
                    'event_action',
                    'event_label',
                    'dimension4',
                    'dimension5',
                    'dimension6',
                    'dimension7',
                    'dimension8',
                    'dimension9',
                    'dimension10',
                    'dimension12',
                    'dimension13',
                    'dimension14',
                    'total_events',
                    'unique_events',
                    'event_value',
                    'goal1_completions',
                    'goal2_completions',
                    'transactions',
                    'transaction_revenue',
                    'sampling']

events_combined.columns = event_nice_names

# where timezone offset out of bounds, remove it
events_combined['dimension3'] = events_combined['dimension3'].swifter.apply(lambda x: rogue_tz_offsets(x))

# Clear memory. Might not be necessary but still haunted from using R
del events1, events2, events3, event_nice_names, events_ordered


print('start uploading events data to Azure')
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
events_combined.to_sql('events',
                       con = engine,
                       schema = 'ga_flagship_ecom',
                       index = False,
                       if_exists = 'append')

