import time
import datetime as dt


def date_string_to_datetime(date_string):
    date_datetime = dt.datetime.strptime(date_string, "%d/%m/%Y")

    return date_datetime


def datetime_to_timestamp_ms(date_datetime):
    date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)

    return date_timestamp
