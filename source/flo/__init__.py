#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_monthly package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
from calendar import monthrange
import logging
import traceback
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_ctp_daily as hirs_ctp_daily

SPC = StoredProductCatalog()

# every module should have a LOG object
LOG = logging.getLogger(__name__)


class HIRS_CTP_MONTHLY(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id',
                  'hirs_ctp_orbital_delivery_id', 'hirs_ctp_daily_delivery_id',
                  'hirs_ctp_monthly_delivery_id']
    outputs = ['out']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                      hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                      hirs_ctp_monthly_delivery_id):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id,
                 'hirs_ctp_orbital_delivery_id': hirs_ctp_orbital_delivery_id,
                 'hirs_ctp_daily_delivery_id': hirs_ctp_daily_delivery_id,
                 'hirs_ctp_monthly_delivery_id': hirs_ctp_monthly_delivery_id}
                for g in granules]

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_MONTHLY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''

        LOG.debug("Running build_task()")

        # Instantiate the hirs_ctp_daily computation
        hirs_ctp_daily_comp = hirs_ctp_daily.HIRS_CTP_DAILY()

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'], context['granule'] + timedelta(num_days),
                                False, True)

        daily_contexts = hirs_ctp_daily_comp.find_contexts(
                                                        interval,
                                                        context['satellite'],
                                                        context['hirs2nc_delivery_id'],
                                                        context['hirs_avhrr_delivery_id'],
                                                        context['hirs_csrb_daily_delivery_id'],
                                                        context['hirs_csrb_monthly_delivery_id'],
                                                        context['hirs_ctp_orbital_delivery_id'],
                                                        context['hirs_ctp_daily_delivery_id'])

        if len(daily_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CTP_DAILY inputs available for {}'.format(context['granule']))

        for (idx, daily_context) in enumerate(daily_contexts):
            hirs_ctp_daily_prod = hirs_ctp_daily_comp.dataset('out').product(daily_context)
            if SPC.exists(hirs_ctp_daily_prod):
                task.input('CTPD-{}'.format(idx), hirs_ctp_daily_prod, True)

    def create_ctp_monthly(self, inputs, context):
        '''
        Create the CTP monthly mean current month.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CTP script locations
        hirs_ctp_monthly_delivery_id = context['hirs_ctp_monthly_delivery_id']
        delivery = delivered_software.lookup('hirs_ctp_monthly', delivery_id=hirs_ctp_monthly_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        version = delivery.version

        # Determine the output filenames
        output_file = 'hirs_ctp_monthly_{}_{}.nc'.format(context['satellite'],
                                                       context['granule'].strftime('D%Y%m'))
        LOG.info("output_file: {}".format(output_file))

        # Generating CTP Daily Input List
        ctp_daily_file = 'ctp_daily_list'
        with open(ctp_daily_file, 'w') as f:
            [f.write('{}\n'.format(basename(input))) for input in inputs.values()]

        # Run the CTP monthly binary
        ctp_monthly_bin = pjoin(dist_root, 'bin/create_monthly_daynight_ctps.exe')
        cmd = '{} {} {}'.format(
                ctp_monthly_bin,
                ctp_daily_file,
                output_file
                )
        #cmd = 'sleep 0.5; touch {}'.format(output_file)

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_ctp = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_ctp = err.returncode
            LOG.error(" CTP monthly binary {} returned a value of {}".format(ctp_monthly_bin, rc_ctp))
            return rc_ctp, None

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.debug('Found output CTP monthly file "{}"'.format(output_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_file))
            rc = 1
            return rc, None

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_MONTHLY')
    def run_task(self, inputs, context):
        '''
        Run the CTP monthly binary on a single context
        '''

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Link the inputs into the working directory
        inputs = symlink_inputs_to_working_dir(inputs)

        # Create the CTP monthly file for the current month.
        rc, ctp_monthly_file = self.create_ctp_monthly(inputs, context)

        return {'out': nc_compress(ctp_monthly_file)}
