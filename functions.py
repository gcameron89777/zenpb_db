import pandas as pd
import connect
from datetime import datetime
import time
from dateutil.rrule import rrule, DAILY
import sys


def convert_response_to_df(response):
    """
    used for each iteration against the api, converts response to a dataframe
    """

    list = []

    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        sampled = True if report.get('samplesReadCounts') else False

        for row in rows:
            dict = {}
            dict['sampling'] = sampled
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
              dict[header] = dimension

            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
                    if ',' in value or '.' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                      dict[metric.get('name')] = int(value)
            list.append(dict)

        df = pd.DataFrame(list)
        return df


def get_report(start_date, end_date, view_id, metrics, dimensions, dimensionFilterClauses=[], segments=[], pageToken=None):
    """
    This is the function that actually hits the api
    """

    return connect.service.reports().batchGet(
        body={
            'reportRequests': [{
              'viewId': view_id,
              'dateRanges': [{'startDate':start_date, 'endDate': end_date}],
              'metrics': metrics,
              'dimensions': dimensions,
              'pageSize': 10000,
              'dimensionFilterClauses': dimensionFilterClauses,
              'segments': segments,
              'pageToken': pageToken
            }]
          }
      ).execute()


def return_ga_data(start_date, end_date, view_id, metrics, dimensions, group_by=[], dimensionFilterClauses=[], segments=[]):
    """
    handles paginated query results and loops over input date range by day to return all results and minimise sampling
    """

    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    final_list = []
    for date in rrule(freq=DAILY, dtstart=start_date, until=end_date):
        date = str(date.date())

        # for paginated results
        page_token = '0'  # for initial iteration this just needs to be anything except None
        while page_token != None:

            # GA API limit of 100 requests per 100 seconds
            time.sleep(1)

            iresponse = get_report(date, date, view_id, metrics, dimensions, dimensionFilterClauses, segments, pageToken=page_token)

            # make sure there are results else quit
            if 'rowCount' not in iresponse['reports'][0]['data']:
                pass

            i_df = convert_response_to_df(iresponse)
            final_list.append(i_df)
            page_token = iresponse['reports'][0].get('nextPageToken')  # update the pageToken

    final_df = pd.concat(final_list)

    if len(group_by) != 0 and final_df.shape[0] > 0:
        final_df = final_df.groupby(group_by).sum().reset_index()
    return final_df
