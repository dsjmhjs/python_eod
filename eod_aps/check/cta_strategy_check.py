# -*- coding: utf-8 -*-
from eod_aps.check import *
import os
from eod_aps.model.eod_const import const

BACKTEST_BASE_PATH_TEMPLATE = const.EOD_CONFIG_DICT['backtest_base_path_template']
check_file_list = ('common.instrument_commission_rate.csv', 'portfolio.pf_account.csv', 'portfolio.pf_position.csv',
                   'om.trade2_history.csv', 'strategy.strategy_state.csv', 'strategy.strategy_parameter.csv')


def backtest_files_export_check(job_name):
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    cta_server_list = server_constant.get_cta_servers()

    error_file_list = []
    for server_name in cta_server_list:
        check_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_name)
        for file_name in check_file_list:
            check_file_path = os.path.join(check_folder, file_name)
            if not os.path.exists(check_file_path):
                error_file_list.append('Miss File:%s' % check_file_path)
                continue

            modify_hour = date_utils.timestamp_tostring(os.stat(check_file_path).st_mtime, '%H')
            now_hour = date_utils.get_today_str('%H')
            if modify_hour != now_hour:
                error_file_list.append('File Time Error:%s' % check_file_path)

    if error_file_list:
        email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name, '\n' % error_file_list)


if __name__ == '__main__':
    print date_utils.get_today_str('%H')




