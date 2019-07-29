# -*- coding: utf-8 -*-
from eod_aps.job import *


def proxy_update_index():
    server_name = 'guoxin'
    account_list = ['0357-PROXY-xhms01-', '0356-PROXY-xhhm02-']
    server_model = server_constant.get_server_model(server_name)

    cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_project_folder'],
                './build64_release/fetcher/fetch_position -a PROXY'
    ]
    server_model.run_cmd_str(';'.join(cmd_list))

    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       'python xt_position_analysis.py'
    ]
    server_model.run_cmd_str(';'.join(update_cmd_list))

    tmp_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder']]
    for account_name in account_list:
        tmp_cmd_list.append('/home/trader/anaconda2/bin/python screen_tools.py -s MainFrame -c "update account %s"' % account_name)
    server_model.run_cmd_str(';'.join(tmp_cmd_list))


if __name__ == '__main__':
    proxy_update_index()