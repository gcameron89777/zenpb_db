# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

## sessions
start_date = '2019-09-06'
end_date = '2019-10-18'

# note that at the time of building this script the timestamp dimension, dimension3, was only granular at the second level.
# Updated this variable in GTM today (10/19/19) to set at the millisecond level as an ISO string, including timezone.
# This level of granularity should be enough for form a key when combined with session id, however for here and now a conbination of
# several additional dimensions must be used to form a key of unique values

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
    {'name': 'ga:eventLabel'}

  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventCategory', 'ga:eventAction', 'ga:eventLabel'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with common dimensions
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

    # event parameters (only pulling in as id while timestamp not currently usable)
    {'name': 'ga:eventAction'},
    {'name': 'ga:eventLabel'},

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension6'}, # sign up method
    {'name': 'ga:dimension7'}, # shoot types
    {'name': 'ga:dimension8'} # introduction
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel', 'ga:dimension6',
              'ga:dimension7', 'ga:dimension8'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with common dimensions
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

    # event parameters (only pulling in as id while timestamp not currently usable)
    {'name': 'ga:eventAction'},
    {'name': 'ga:eventLabel'},

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension9'}, # availability
    {'name': 'ga:dimension10'}, # rate
    {'name': 'ga:dimension11'} # site search
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel', 'ga:dimension9',
              'ga:dimension10', 'ga:dimension11'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with common dimensions
events4 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:totalEvents'} # will drop afterwards since already have in events1, only need a metric to hit the api with
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # event parameters (only pulling in as id while timestamp not currently usable)
    {'name': 'ga:eventAction'},
    {'name': 'ga:eventLabel'},

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension14'}, # availability
    {'name': 'ga:dimension16'} # review
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel', 'ga:dimension14',
              'ga:dimension16'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with common dimensions
events5 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:totalEvents'} # will drop afterwards since already have in events1, only need a metric to hit the api with
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension3'}, # timestamp

    # event parameters (only pulling in as id while timestamp not currently usable)
    {'name': 'ga:eventAction'},
    {'name': 'ga:eventLabel'},

    # custom hit scoped dimensions for events
    {'name': 'ga:dimension4'},  # account id
    {'name': 'ga:dimension5'}  # travel distance
  ],
  group_by = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel', 'ga:dimension4', 'ga:dimension5'],
  dimensionFilterClauses = [],
  segments=[]
)


# join
## for sampling flag use result from events1. Same time frame so result will be the same regardless
events_combined = events1.merge(events2.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                   on = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel'],
                                   how = 'left').merge(events3.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                                       on = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel'],
                                                       how = 'left').merge(events4.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                                                           on = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel'],
                                                                           how = 'left').merge(events5.drop(['ga:totalEvents', 'sampling'], axis = 1),
                                                                                               on = ['ga:dimension1', 'ga:dimension3', 'ga:eventAction', 'ga:eventLabel'],
                                                                                               how = 'left')

# some leading or trailing whitespace on some fields causing dups
# remove leading or trailing whitespace then dedup
events_combined = events_combined.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
events_combined.fillna('na', inplace=True)
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
                  'ga:dimension11',
                  'ga:dimension14',
                  'ga:dimension16',
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
                    'dimension11',
                    'dimension14',
                    'dimension16',
                    'total_events',
                    'unique_events',
                    'event_value',
                    'goal1_completions',
                    'goal2_completions',
                    'transactions',
                    'transaction_revenue',
                    'sampling']

events_combined.columns = event_nice_names

# Clear memory. Might not be necessary but still haunted from using R
del events1, events2, events3, events4, events5, event_nice_names, events_ordered

# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
events_combined.to_sql('events',
                       con = engine,
                       schema = 'ga_photo_booker',
                       index = False,
                       if_exists = 'append')

