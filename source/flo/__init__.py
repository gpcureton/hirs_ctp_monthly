from calendar import monthrange
from datetime import datetime, timedelta
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import TimeInterval
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs_ctp_daily import HIRS_CTP_DAILY

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


class HIRS_CTP_MONTHLY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    def build_task(self, context, task):

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'], context['granule'] + timedelta(num_days),
                                False, True)

        daily_contexts = HIRS_CTP_DAILY().find_contexts(context['sat'], context['hirs_version'],
                                                        context['collo_version'],
                                                        context['csrb_version'],
                                                        context['ctp_version'], interval)

        for (i, c) in enumerate(daily_contexts):
            task.input('CTPD-{}'.format(i), HIRS_CTP_DAILY().dataset('out').product(c), True)

    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        output = 'ctp.monthly.{}.{}.nc'.format(context['sat'], context['granule'].strftime('D%y%j'))

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

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      time_interval):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g, 'sat': sat, 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_MONTHLY')
