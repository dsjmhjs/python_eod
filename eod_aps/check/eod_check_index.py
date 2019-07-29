# -*- coding: utf-8 -*-
# 用于配置各定时任务的执行后检查
from eod_aps.check.server_service_status_check import *
from eod_aps.check.cta_strategy_check import *
from eod_aps.check.other_check import *
from eod_aps.check.download_log_check import *
from eod_aps.model.eod_const import const
from eod_aps.tools.email_utils import EmailUtils

skip_job_list = ('download_ctp_market_file_am', 'not_daily_reader', 'daily_sharry_writer1', 'basic_sharry',
                 'index_bin', 'download_tradeplat_log', 'db_pre_update_am', 'db_pre_update_pm', 'reload_pickle_data',
                 'server_connection_monitor_am', '')
email_utils2 = EmailUtils(const.EMAIL_DICT['group2'])


class EodCheckIndex(object):
    def __init__(self, job_name):
        self.__job_name = job_name

    def start_check_index(self):
        custom_log.log_info_task('--------------Check[%s] Start.--------------' % self.__job_name)
        if self.__job_name in skip_job_list:
            pass
        elif self.__job_name in ('kill_aggregator', 'kill_aggregator_pm'):
            kill_aggregator_check(self.__job_name)
        elif self.__job_name in ('start_aggregator_am', 'start_aggregator_pm'):
            start_aggregator_check(self.__job_name)
        elif self.__job_name == 'update_strategy_online_am':
            # TODO
            pass
        elif self.__job_name in ('backtest_files_export_am', 'backtest_files_export_pm'):
            backtest_files_export_check(self.__job_name)
        elif self.__job_name in ('start_server_am', 'start_server_pm'):
            start_service_check(self.__job_name)
        elif self.__job_name in ('stop_service_am', 'stop_service_pm'):
            stop_service_check(self.__job_name)
        elif self.__job_name in ('download_msci_file',):
            msci_file_check(self.__job_name)
        elif self.__job_name in ('factordata_file_rebuild',):
            factordata_file_check(self.__job_name)
        elif self.__job_name in ('volume_profile_upload',):
            volume_profile_upload_check(self.__job_name)
        elif self.__job_name in ('special_tickers_init',):
            special_tickers_init_check(self.__job_name)
        elif self.__job_name in ('download_deposit_server_log'):
            download_depositplat_log_check(self.__job_name)
        elif self.__job_name in ('clear_deposit_ftp_job'):
            clear_deposit_ftp_check(self.__job_name)
        else:
            # email_utils2.send_email_group_all('[ERROR]After Check Miss!Job:%s' % self.__job_name, '')
            pass
        custom_log.log_info_task('--------------Check[%s] Stop.--------------' % self.__job_name)
