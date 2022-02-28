import math
import pandas as pd
import datetime
from pathlib import Path


def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    if decimals <= 0:
        return int(math.floor(n * multiplier) / multiplier)
    else:
        return math.floor(n * multiplier) / multiplier


def round_up(n, decimals=0):
    multiplier = 10 ** decimals
    if decimals <= 0:
        return int(math.ceil(n * multiplier) / multiplier)
    else:
        return math.ceil(n * multiplier) / multiplier


def round_time(date_delta, dt, to="average"):

    round_to = date_delta.total_seconds()

    if dt is None:
        dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)
