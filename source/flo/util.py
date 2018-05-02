
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


def generate_cfsr_bin(package_root):

    shutil.copy(os.path.join(package_root, 'bin/wgrib2'), './')

    # Search for the old style pgbhnl.gdas.*.grb2 files
    LOG.debug("Searching for pgbhnl.gdas.*.grb2 ...")
    files = glob('pgbhnl.gdas.*.grb2')
    LOG.debug("... found {}".format(files))

    # Search for the new style cdas1.*.t*z.pgrbhanl.grib2
    if len(files)==0:
        LOG.debug("Searching for cdas1.*.pgrbhanl.grib2 ...")
        files = glob('cdas1.*.pgrbhanl.grib2')
        LOG.debug("... found {}".format(files))

    LOG.debug("CFSR files: {}".format(files)) # GPC

    new_cfsr_files = []
    for file in files:
        cmd = os.path.join(package_root, 'bin/extract_cfsr.csh')
        cmd += ' {} {}.bin ./'.format(file, file)

        LOG.debug(cmd)

        try:
            check_call(cmd, shell=True)
            new_cfsr_files.append('{}.bin'.format(file))
        except:
            pass

    return new_cfsr_files
