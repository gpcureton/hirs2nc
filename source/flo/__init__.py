#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs2nc package

Copyright (c) 2018 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
import logging
import traceback

import flo
from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta
from flo.util import augmented_env
from subprocess import CalledProcessError

import sipsprod
from glutil import (
    check_call,
    #dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
from flo.sw.hirs2nc.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS2NC(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id']
    outputs = ['out']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.HIRX')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        sensor = 'hirs'
        satellite =  context['satellite']
        file_type = 'HIR1B'
        granule = context['granule']

        task.input('HIR1B', delta_catalog.file(sensor, satellite, file_type, granule))

    def local_prepare_env(self, dist_root, inputs, context):
        LOG.debug("Running local_prepare_env()...")

        LOG.debug("package_root = {}".format(self.package_root))
        LOG.debug("dist_root = {}".format(dist_root))

        env = dict(os.environ)
        envroot = pjoin(dist_root, 'env')

        LOG.debug("envroot = {}".format(envroot))

        env['LD_LIBRARY_PATH'] = ':'.join([pjoin(envroot, 'lib'),
                                           pjoin(dist_root, 'lib')])
        env['PATH'] = ':'.join([pjoin(envroot, 'bin'),
                                pjoin(dist_root, 'bin'),
                                '/usr/bin:/bin'])

        LOG.debug("env['PATH'] = \n\t{}".format(env['PATH'].replace(':','\n\t')))
        LOG.debug("env['LD_LIBRARY_PATH'] = \n\t{}".format(env['LD_LIBRARY_PATH'].replace(':','\n\t')))

        return env

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.HIRX')
    def run_task(self, inputs, context):

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        granule = context['granule']
        satellite = context['satellite']
        hirs2nc_delivery_id = context['hirs2nc_delivery_id']
        hirs_version = self.satellite_version(context['satellite'], context['granule'])

        # Get the location of the binary package
        delivery = delivered_software.lookup('hirs2nc', delivery_id=hirs2nc_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        envroot = pjoin(dist_root, 'env')

        # Get the required  environment variables
        env = prepare_env([delivery])
        LOG.debug(env)

        # What is the path of the python interpreter
        py_interp = "{}/bin/python".format(envroot)
        LOG.debug("py_interp = '{}'".format(py_interp))

        # Path of the hirs2nc binary
        hirs2nc_bin = pjoin(envroot, 'bin', 'hirs2nc')

        # Where are we running the package
        work_dir = abspath(curdir)
        LOG.debug("working dir = {}".format(work_dir))

        input_file = inputs['HIR1B']
        output_file = pjoin(work_dir, basename('{}.nc'.format(inputs['HIR1B'])))
        LOG.debug("Input file = {}".format(input_file))
        LOG.debug("Output file = {}".format(output_file))

        # What are our inputs?
        for input in inputs.keys():
            inputs_dir = dirname(inputs[input])
            LOG.debug("inputs['{}'] = {}".format(input,inputs[input]))
        LOG.debug("Inputs dir = {}".format(inputs_dir))

        # Convert the flat HIRS file to NetCDF4
        cmd = '{} {} {} {} {}'.format(
                py_interp,
                hirs2nc_bin,
                hirs_version,
                input_file,
                output_file
                )
        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_hirs2nc = 0
            runscript(cmd, requirements=[], env=env)
        except CalledProcessError as err:
            rc_hirs2nc = err.returncode
            LOG.error("hirs2nc binary {} returned a value of {}".format(hirs2nc_bin, rc_hirs2nc))
            return rc_hirs2nc, []

        # The staging routine assumes that the output file is located in the work directory
        # "tmp******", and that the output path is to be prepended, so return the basename.
        output = basename('{}.nc'.format(inputs['HIR1B']))

        return {'out': nc_compress(output)}

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id):

        global delta_catalog

        LOG.debug('delta_catalog.collection = {}'.format(delta_catalog.collection))
        LOG.debug('delta_catalog.input_data = {}'.format(delta_catalog.input_data))

        files = delta_catalog.files('hirs', satellite, 'HIR1B', time_interval)

        return [{'granule': file.data_interval.left,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id}
                for file in files
                if file.data_interval.left >= time_interval.left]

    def satellite_version(self, satellite, granule):

        # 4 for everything post April 28, 2005
        if granule > datetime(2005, 4, 28):
            return 4
        # 3 for noaa-15,16,17 until April 28, 2005
        elif satellite in ['noaa-15', 'noaa-16', 'noaa-17']:
            return 3
        # 2 for noaa <= 14
        else:
            return 2
