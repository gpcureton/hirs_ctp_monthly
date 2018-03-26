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

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    #delivered_software,
    #support_software,
    #runscript,
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

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_MONTHLY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))

        # Instantiate the hirs_csrb_daily computation
        hirs_ctp_daily_comp = hirs_ctp_daily.HIRS_CTP_DAILY()

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'], context['granule'] + timedelta(num_days),
                                False, True)

        daily_contexts = hirs_ctp_daily_comp.find_contexts(
                                                        interval,
                                                        context['sat'],
                                                        context['hirs_version'],
                                                        context['collo_version'],
                                                        context['csrb_version'],
                                                        context['ctp_version'])

        if len(daily_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CTP_DAILY inputs available for {}'.format(context['granule']))

        for (i, daily_context) in enumerate(daily_contexts):
            hirs_ctp_daily_prod = hirs_ctp_daily_comp.dataset('out').product(daily_context)
            if SPC.exists(hirs_ctp_daily_prod):
                task.input('CTPD-{}'.format(i), hirs_ctp_daily_prod, True)

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_MONTHLY')
    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        output = 'ctp.monthly.{}.{}.nc'.format(context['sat'], context['granule'].strftime('d%Y%m'))

        # Generate CTP Daily Input List
        ctp_daily_file = 'ctp_daily_list'
        with open(ctp_daily_file, 'w') as f:
            [f.write('{}\n'.format(input)) for input in inputs.values()]

        cmd = os.path.join(self.package_root, context['ctp_version'],
                           'bin/create_monthly_daynight_ctps.exe')
        cmd += ' {} {}'.format(ctp_daily_file, output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, time_interval, sat, hirs_version, collo_version, csrb_version, ctp_version):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_MONTHLY')
