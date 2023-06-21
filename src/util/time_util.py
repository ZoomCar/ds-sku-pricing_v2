import functools
import time
from datetime import datetime

from dateutil import tz


def timeit(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        print('function [{}] finished in {} ms'.format(
            func.__name__, int(elapsedTime * 1000)))

    return newfunc


def get_ist_from_utc(dt: datetime):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Kolkata')
    dt = dt.replace(tzinfo=from_zone)
    ist_dt = dt.astimezone(to_zone)
    return ist_dt


def set_time_zone_as_ist(dt: datetime):
    to_zone = tz.gettz('Asia/Kolkata')
    ist_dt = dt.astimezone(to_zone)
    return ist_dt


def get_current_timestamp_ist():
    dt = datetime.utcnow()
    return get_ist_from_utc(dt)
