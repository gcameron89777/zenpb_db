import dateutil
import pandas as pd
import connect
from datetime import datetime
import time
from dateutil.rrule import rrule, DAILY, parser
import sys
import random
import time
from apiclient.errors import HttpError
import socket
socket.setdefaulttimeout(900)  # set timeout to 10 minutes


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
    # exponential back off
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors#backoff
    load_request = {
            'reportRequests': [{
              'viewId': view_id,
              'dateRanges': [{'startDate':start_date, 'endDate': end_date}],
              'metrics': metrics,
              'dimensions': dimensions,
              'pageSize': 100000,
              'dimensionFilterClauses': dimensionFilterClauses,
              'segments': segments,
              'pageToken': pageToken
            }]
          }
    for n in range(0, 5):
        try:
            return connect.service.reports().batchGet(
                body = load_request
            ).execute()
        except HttpError as error:
            if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded', 'internalServerError', 'backendError']:
                time.sleep((2 ** n) + random.random())
            elif 'The service is currently unavailable' in str(error):
                time.sleep((2 ** n) + random.random())
    print("There has been an error, the request never succeeded.")


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

            # for tracking progress in logs
            print("pageToken is:" + page_token + " : " + date)

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


def rogue_tz_offsets(t):
    """
    ValueError: Cannot mix tz-aware with tz-naive values, looks like  aPostgres thing
    Just remove the tz offset on all of them

    Handle nonsensical timezone offsets and also input string e.g. 'other' from API
    :param t: input timestamp string
    :return: a datetime, some with offset
    """

    try:
        return dateutil.parser.parse(t).replace(tzinfo=None).isoformat()
    except ValueError as e:
        if 'Unknown string format' in str(e):
            return dateutil.parser.parse('2100-01-01 00:00:00').isoformat()
    except AttributeError as e:
        if 'NoneType' in str(e):
            return dateutil.parser.parse(t)

    # try:
    #     d = dateutil.parser.parse(t)
    #     if -12 * 60 * 60 <= d.utcoffset().total_seconds() <= 14 * 60 * 60:
    #         return d.isoformat()
    #     return d.replace(tzinfo=None).isoformat()
    # except ValueError as e:
    #    if 'Unknown string format' in str(e):
    #        return dateutil.parser.parse('2100-01-01 00:00:00').isoformat()
    #    if 'offset must be a timedelta strictly between' in str(e):
    #        return dateutil.parser.parse(t).replace(tzinfo=None).isoformat()
    # except AttributeError as e:
    #     if 'NoneType' in str(e):
    #         return dateutil.parser.parse(t)