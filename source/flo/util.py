
from datetime import datetime, timedelta
from glob import glob
import shutil
import os
from flo.subprocess import check_call
from flo.time import TimeInterval

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__name__)


def hirs_to_time_interval(filename):

    begin_time = datetime.strptime(filename[12:24], 'D%y%j.S%H%M')
    end_time = datetime.strptime(filename[12:19]+filename[25:30], 'D%y%j.E%H%M')
    if end_time < begin_time:
        end_time += timedelta(days=1)

    return TimeInterval(begin_time, end_time)


def time_interval_to_hirs(interval):

    return '{}.{}'.format(interval.left.strftime('D%y%j.S%H%M'),
                          interval.right.strftime('E%H%M'))
