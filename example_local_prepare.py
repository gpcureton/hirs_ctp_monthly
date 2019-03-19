#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_monthly package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import sys
import traceback
import logging

from timeutil import TimeInterval, datetime, timedelta
from flo.ui import local_prepare, local_execute

import flo.sw.hirs_ctp_daily as hirs_ctp_daily
import flo.sw.hirs_ctp_monthly as hirs_ctp_monthly

from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

#
# General information
#

#hirs2nc_delivery_id = '20180410-1'
#hirs_avhrr_delivery_id = '20180505-1'
#hirs_csrb_daily_delivery_id  = '20180714-1'
#hirs_csrb_monthly_delivery_id  = '20180516-1'
#hirs_ctp_orbital_delivery_id  = '20180730-1'
#hirs_ctp_daily_delivery_id  = '20180802-1'
#hirs_ctp_monthly_delivery_id  = '20180803-1'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information


#
# Local execution
#

def local_execute_example(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                          hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                          hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                          hirs_ctp_monthly_delivery_id,
                          skip_prepare=False, skip_execute=False, single=True, verbosity=2):

    setup_logging(verbosity)

    hirs_ctp_daily_comp = hirs_ctp_daily.HIRS_CTP_DAILY()
    comp = hirs_ctp_monthly.HIRS_CTP_MONTHLY()

    # Get the required context...
    contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                                  hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                                  hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                                  hirs_ctp_monthly_delivery_id)

    if len(contexts) != 0:
        LOG.info("Candidate contexts in interval...")
        for context in contexts:
            print("\t{}".format(context))

        if not single:

            for idx,context in enumerate(contexts):
                LOG.info('Current Dir: {} {}'.format(idx, os.getcwd()))
                try:
                    if not skip_prepare:
                        LOG.info("Running hirs_ctp_monthly local_prepare()...")
                        LOG.info("Preparing context... {}".format(context))
                        local_prepare(comp, context, download_onlies=[hirs_ctp_daily_comp])
                    if not skip_execute:
                        LOG.info("Running hirs_ctp_monthly local_execute()...")
                        LOG.info("Running context... {}".format(context))
                        local_execute(comp, context, download_onlies=[hirs_ctp_daily_comp])

                    if not skip_prepare:
                        shutil.move('inputs', 'inputs_{}'.format(idx))
                    if not skip_execute:
                        shutil.move('outputs', 'outputs_{}'.format(idx))

                except Exception, err:
                    LOG.error("{}".format(err))
                    LOG.debug(traceback.format_exc())

        else:

            LOG.info("Single context!")
            try:
                if not skip_prepare:
                    LOG.info("Running hirs_ctp_monthly local_prepare()...")
                    LOG.info("Preparing context... {}".format(contexts[0]))
                    local_prepare(comp, contexts[0], download_onlies=[hirs_ctp_daily_comp])
                if not skip_execute:
                    LOG.info("Running hirs_ctp_monthly local_execute()...")
                    LOG.info("Running context... {}".format(contexts[0]))
                    local_execute(comp, contexts[0], download_onlies=[hirs_ctp_daily_comp])
            except Exception, err:
                LOG.error("{}".format(err))
                LOG.debug(traceback.format_exc())
    else:
        LOG.error("There are no valid {} contexts for the interval {}.".format(satellite, interval))

def print_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                   hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                   hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                   hirs_ctp_monthly_delivery_id, verbosity=2):

    setup_logging(verbosity)
    
    comp = hirs_ctp_monthly.HIRS_CTP_MONTHLY()

    contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                                  hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                                  hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                                  hirs_ctp_monthly_delivery_id)
    LOG.info('Printing contexts...')
    for context in contexts:
        LOG.info(context)
