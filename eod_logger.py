# -*- coding: utf-8 -*-
import traceback
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.eod_const import const
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.check.eod_check_index import EodCheckIndex
from cfg import *


email_utils = EmailUtils(const.EMAIL_DICT['group2'])
date_utils = DateUtils()


def __eod_start_job(job_name):
    custom_log.log_info_task('--------------Job[%s]Start.--------------' % job_name)


def __eod_end_job(job_name):
    custom_log.log_info_task('==============Job[%s]Stop!==============' % job_name)


def log_trading_wrapper(func):
    def log(*args, **kwargs):
        try:
            const.JOB_START_TIME_DICT[func.__name__] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
            __eod_start_job(func.__name__)
            if not date_utils.is_trading_day():
                custom_log.log_info_task("Job[%s],Today's Not TradingDay!" % func.__name__)
            else:
                func(*args, **kwargs)
            __eod_end_job(func.__name__)
            const.JOB_END_TIME_DICT[func.__name__] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')

            # 对定时任务增加异步的校验逻辑
            eod_check_index = EodCheckIndex(func.__name__)
            eod_check_index.start_check_index()
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_task(error_msg)
            email_utils.send_email_group_all('[Error]Running Error Job:%s!' % func.__name__, error_msg)
    return log


def log_wrapper(func):
    def log(*args, **kwargs):
        try:
            const.JOB_START_TIME_DICT[func.__name__] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
            __eod_start_job(func.__name__)
            func(*args, **kwargs)
            __eod_end_job(func.__name__)
            const.JOB_END_TIME_DICT[func.__name__] = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')

            # 对定时任务增加异步的校验逻辑
            eod_check_index = EodCheckIndex(func.__name__)
            eod_check_index.start_check_index()
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_task(error_msg)
            email_utils.send_email_group_all('[Error]Running Error Job:%s!' % func.__name__, error_msg)
    return log
