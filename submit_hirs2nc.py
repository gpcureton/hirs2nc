#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs2nc package

Copyright (c) 2018 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import traceback
import calendar
from time import sleep
import logging

from flo.ui import safe_submit_order
from timeutil import TimeInterval, datetime, timedelta

import flo.sw.hirs2nc as hirs2nc
from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

setup_logging(2)

# General information
hirs2nc_delivery_id  = '20180410-1'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information

satellite_files = {
    'noaa-06': ['NSS.GHRR.NA.D80182.S0531.E0724.B0523940.WI.level2.hdf', 'NSS.GHRR.NA.D82214.S2135.E2304.B1610506.WI.level2.hdf'],
    'noaa-07': ['NSS.GHRR.NC.D81236.S0013.E0207.B0086970.WI.level2.hdf', 'NSS.GHRR.NC.D85032.S2221.E0009.B1863132.GC.level2.hdf'],
    'noaa-08': ['NSS.GHRR.NE.D83124.S1909.E2046.B0052829.GC.level2.hdf', 'NSS.GHRR.NE.D85287.S0326.E0511.B1322829.GC.level2.hdf'],
    'noaa-09': ['NSS.GHRR.NF.D85056.S0013.E0154.B0105253.GC.level2.hdf', 'NSS.GHRR.NF.D88311.S2319.E0107.B2011011.GC.level2.hdf'],
    'noaa-10': ['NSS.GHRR.NG.D87320.S2321.E0115.B0604748.GC.level2.hdf', 'NSS.GHRR.NG.D90259.S2346.E0139.B2076668.GC.level2.hdf'],
    'noaa-11': ['NSS.GHRR.NH.D88313.S0016.E0210.B0062930.WI.level2.hdf', 'NSS.GHRR.NH.D94243.S2334.E0122.B3059293.GC.level2.hdf'],
    'noaa-12': ['NSS.GHRR.ND.D91259.S0017.E0211.B0176668.GC.level2.hdf', 'NSS.GHRR.ND.D98348.S2043.E2226.B3939697.WI.level2.hdf'],
    'noaa-14': ['NSS.GHRR.NJ.D00001.S0001.E0121.B2578687.GC.level2.hdf', 'NSS.GHRR.NJ.D99365.S2205.E2359.B2578586.GC.level2.hdf'],
    'noaa-15': ['NSS.GHRR.NK.D00001.S0001.E0152.B0849596.GC.level2.hdf', 'NSS.GHRR.NK.D99365.S2220.E2359.B0849494.WI.level2.hdf'],
    'noaa-16': ['NSS.GHRR.NL.D01078.S2259.E0047.B0253233.GC.level2.hdf', 'NSS.GHRR.NL.D06200.S2248.E0033.B3002122.GC.level2.hdf'],
    'noaa-17': ['NSS.GHRR.NM.D02235.S2229.E0017.B0085657.GC.level2.hdf', 'NSS.GHRR.NM.D09010.S2325.E0055.B3403839.WI.level2.hdf'],
    'noaa-18': ['NSS.GHRR.NN.D05200.S0113.E0308.B0084041.WI.level2.hdf', 'NSS.GHRR.NN.D17365.S2218.E2359.B6501718.WI.level2.hdf'],
    'noaa-19': ['NSS.GHRR.NP.D09108.S2301.E0050.B0100809.GC.level2.hdf', 'NSS.GHRR.NP.D17365.S2238.E2359.B4585757.GC.level2.hdf'],
    'metop-a': ['NSS.GHRR.M2.D07179.S2314.E0058.B0358485.SV.level2.hdf', 'NSS.GHRR.M2.D17365.S2241.E0024.B5812425.SV.level2.hdf'],
    'metop-b': ['NSS.GHRR.M1.D13140.S0029.E0127.B0347172.SV.level2.hdf', 'NSS.GHRR.M1.D15365.S2307.E0004.B1705253.SV.level2.hdf']
}

satellite_dt = {}                                                                                                                            
for sat in satellite_files.keys():
    satellite_dt[sat] = [datetime.strptime(satfile.split('.')[3],'D%y%j') for satfile in satellite_files[sat]]

satellite = 'noaa-19'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + wedge - wedge)]
# NSS.GHRR.NP.D09108.S2301.E0050.B0100809.GC.level2.hdf --> NSS.GHRR.NP.D17365.S2238.E2359.B4585757.GC.level2.hdf
# datetime(2009, 4, 18, 0, 0) --> datetime(2017, 12, 31, 0, 0)
intervals = []
#for years in range(2009, 2018):
for years in range(2014, 2018):
    intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]

#satellite = 'metop-b'
#granule = datetime(2015, 1, 1, 0)
#intervals = [TimeInterval(granule, granule+2*day-wedge)]
# NSS.GHRR.M1.D13140.S0029.E0127.B0347172.SV.level2.hdf --> NSS.GHRR.M1.D15365.S2307.E0004.B1705253.SV.level2.hdf
# datetime(2013, 5, 20, 0, 0) --> datetime(2015, 12, 31, 0, 0)
#intervals = []
#for years in range(2013, 2016):
    #intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]

satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

def setup_computation(satellite):

    input_data = {'HIR1B': '/mnt/software/flo/hirs_l1b_datalists/{0:}/HIR1B_{0:}_latest'.format(satellite),
                  'CFSR':  '/mnt/cephfs_data/geoffc/hirs_data_lists/CFSR.out',
                  'PTMSX': '/mnt/software/flo/hirs_l1b_datalists/{0:}/PTMSX_{0:}_latest'.format(satellite)}

    # Data locations
    collection = {'HIR1B': 'ILIAD',
                  'CFSR': 'DELTA',
                  'PTMSX': 'FJORD'}

    input_sources = {'collection':collection, 'input_data':input_data}

    # Initialize the hirs2nc module with the data locations
    hirs2nc.set_input_sources(input_sources)

    # Instantiate the computation
    comp = hirs2nc.HIRS2NC()

    return comp

LOG.info("Submitting intervals...")

dt = datetime.utcnow()
log_name = 'hirs2nc_{}_s{}_e{}_c{}.log'.format(
    satellite,
    intervals[0].left.strftime('%Y%m%d%H%M'),
    intervals[-1].right.strftime('%Y%m%d%H%M'),
    dt.strftime('%Y%m%d%H%M%S'))

try:

    for interval in intervals:
        LOG.info("Submitting interval {} -> {}".format(interval.left, interval.right))

        comp = setup_computation(satellite)

        contexts =  comp.find_contexts(interval, satellite, hirs2nc_delivery_id)

        LOG.info("Opening log file {}".format(log_name))
        file_obj = open(log_name,'a')

        LOG.info("\tThere are {} contexts in this interval".format(len(contexts)))
        contexts.sort()

        if contexts != []:
            #for context in contexts:
                #LOG.info(context)

            LOG.info("\tFirst context: {}".format(contexts[0]))
            LOG.info("\tLast context:  {}".format(contexts[-1]))

            try:
                job_nums = []
                job_nums = safe_submit_order(comp, [comp.dataset('out')], contexts)

                if job_nums != []:
                    #job_nums = range(len(contexts))
                    #LOG.info("\t{}".format(job_nums))

                    file_obj.write("contexts: [{}, {}]; job numbers: [{}..{}]\n".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("contexts: [{}, {}]; job numbers: [{},{}]".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("job numbers: [{}..{}]\n".format(job_nums[0],job_nums[-1]))
                else:
                    LOG.info("contexts: [{}, {}]; --> no jobs\n".format(contexts[0], contexts[-1]))
                    file_obj.write("contexts: [{}, {}]; --> no jobs\n".format(contexts[0], contexts[-1]))
            except Exception:
                LOG.warning(traceback.format_exc())

            sleep(5.)

        LOG.info("Closing log file {}".format(log_name))
        file_obj.close()

except Exception:
    LOG.warning(traceback.format_exc())
