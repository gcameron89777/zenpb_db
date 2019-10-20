# packages and globals
from functions import return_ga_data
import credentials as creds
import psycopg2
from sqlalchemy import create_engine

VIEW_ID = creds.pb_viewid

## sessions
start_date = '2019-09-01'
end_date = '2019-10-18'


# common session dims and all metrics
sessions1 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:sessions'},
    {'expression': 'ga:bounces'},
    {'expression': 'ga:sessionDuration'},
    {'expression': 'ga:pageViews'},
    {'expression': 'ga:goal1completions'},
    {'expression': 'ga:goal2completions'},
    {'expression': 'ga:transactions'},
    {'expression': 'ga:transactionRevenue'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session ID
    {'name': 'ga:dimension2'}, # client ID
    {'name': 'ga:date'},
    {'name': 'ga:hour'},
    {'name': 'ga:minute'},
    {'name': 'ga:landingPagePath'},
    {'name': 'ga:deviceCategory'}
  ],
  group_by = ['ga:dimension1', 'ga:dimension2', 'ga:date', 'ga:hour', 'ga:minute',
              'ga:landingPagePath', 'ga:deviceCategory'],
  dimensionFilterClauses = [],
  segments=[]
)


## continue with common dimensions
sessions2 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:sessions'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session id
    {'name': 'ga:userType'},
    {'name': 'ga:operatingSystem'},
    {'name': 'ga:operatingSystemVersion'}
  ],
  group_by = ['ga:dimension1', 'ga:userType', 'ga:operatingSystem', 'ga:operatingSystemVersion'],
  dimensionFilterClauses = [],
  segments=[]
)


## channel and source data
sessions3 = return_ga_data(
  start_date = start_date,
  end_date = end_date,
  view_id = VIEW_ID,
  metrics = [
    {'expression': 'ga:sessions'}
  ],
  dimensions = [
    {'name': 'ga:dimension1'}, # session id
    {'name': 'ga:source'},
    {'name': 'ga:medium'},
    {'name': 'ga:campaign'},
    {'name': 'ga:adContent'},
    {'name': 'ga:keyword'},
    {'name': 'ga:channelGrouping'}
  ],
  group_by = ['ga:dimension1', 'ga:source', 'ga:medium', 'ga:campaign',
              'ga:adContent', 'ga:keyword', 'ga:channelGrouping'],
  dimensionFilterClauses = [],
  segments=[]
)

# join
## for sampling flag use result from sessions1. Same time frame so result will be the same regardless
sessions_combined = sessions1.merge(sessions2.drop(['ga:sessions', 'sampling'], axis = 1),
                                   on = 'ga:dimension1',
                                   how = 'inner').merge(sessions3.drop(['ga:sessions', 'sampling'], axis = 1),
                                                       on = 'ga:dimension1',
                                                       how = 'inner')

# dedup sessions. Pulling in hour and minute seems to cause duplicates, filtering out where sessions = 0 seems to match GA UI
# on second thought leave in, want to match interface and can always filter in sql
# on third thought put it back in because session id is the key and leaving it out causes issues
sessions_combined = sessions_combined[sessions_combined['ga:sessions'] > 0]
sessions_combined.fillna('na', inplace = True) # replace NaNs wit 'na' if any

## Get fields in order
sessions_ordered = ['ga:dimension1',
                    'ga:dimension2',
                    'ga:date',
                    'ga:hour',
                    'ga:minute',
                    'ga:userType',
                    'ga:deviceCategory',
                    'ga:operatingSystem',
                    'ga:operatingSystemVersion',
                    'ga:landingPagePath',
                    'ga:source',
                    'ga:medium',
                    'ga:campaign',
                    'ga:adContent',
                    'ga:keyword',
                    'ga:channelGrouping',
                    'ga:sessions',
                    'ga:bounces',
                    'ga:sessionDuration',
                    'ga:pageViews',
                    'ga:goal1completions',
                    'ga:goal2completions',
                    'ga:transactions',
                    'ga:transactionRevenue',
                    'sampling']

sessions_combined = sessions_combined[sessions_ordered]

nice_names = ['dimension1',
              'dimension2',
              'date',
              'hour',
              'minute',
              'user_type',
              'device_category',
              'operating_system',
              'operating_system_version',
              'landing_page_path',
              'source',
              'medium',
              'campaign',
              'ad_content',
              'keyword',
              'channel_grouping',
              'sessions',
              'bounces',
              'session_duration',
              'pageviews',
              'goal1_completions',
              'goal2_completions',
              'transactions',
              'transaction_revenue',
              'sampling']

sessions_combined.columns = nice_names

# Clear memory. Might not be necessary but still haunted from using R
del sessions1, sessions2, sessions3, nice_names, sessions_ordered


# Postgres
engine = create_engine('postgresql://' +
                       creds.user + ':' +
                       creds.pw + '@' +
                       creds.host + ':' +
                       creds.port + '/' +
                       creds.db,
                       echo=False)

# post to sessions table
sessions_combined.to_sql('sessions',
                         con = engine,
                         schema = 'ga_photo_booker',
                         index = False,
                         if_exists = 'append')

