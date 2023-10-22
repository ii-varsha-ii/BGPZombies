import calendar
from datetime import datetime, timedelta

import pandas as pd


def get_dates(start_date, end_date):
    return [d.strftime('%Y%m%d') for d in
            pd.date_range(start_date, end_date)]


def get_utc_timestamp(date):
    return [calendar.timegm(datetime.strptime(date, "%Y%m%d %H").utctimetuple()),
            calendar.timegm((datetime.strptime(date, "%Y%m%d %H") + timedelta(hours=2)).utctimetuple())]


def intervals_str(announce, withdrawal):
    return ','.join([announce, withdrawal])
