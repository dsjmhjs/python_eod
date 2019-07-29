# -*- coding: utf-8 -*-
# TS工具，完成TS持仓文件上传，更新持仓，MainFrame中执行命令更新账号信息等动作
import os
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.eod_const import const

date_utils = DateUtils()
server_host = server_constant.get_server_model('host')
TS_FILE_FOLDER_DICT_STR = const.EOD_CONFIG_DICT['ts_file_folder_dict']


class Ts_Parameter(object):
    """
        Ts持仓更新工具
    """
    server_name = None
    ts_file_path = None
    account_list = []

    def __init__(self):
        pass


def ts_update_tool(ts_parameter):
    now_date_str = date_utils.get_today_str('%Y-%m-%d')

    ts_file_list = []
    for file_name in os.listdir(ts_parameter.ts_file_path):
        if now_date_str not in file_name or not file_name.endswith('.txt') or 'ysposition' not in file_name:
            continue
        ts_file_list.append(file_name)

    ts_file_list.sort(key=lambda obj: obj.split('.')[2].zfill(6))
    ts_file_name = ts_file_list[-1]
    print 'start read file:', ts_file_name

    if os.path.getsize(ts_parameter.ts_file_path + '/' + ts_file_name) == 0:
        raise Exception(u"文件生成异常")

    # 刪除之前上传的文件
    server_model = server_constant.get_server_model(ts_parameter.server_name)
    del_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                    'rm -rf ysposition*.txt'
    ]
    server_model.run_cmd_str(';'.join(del_cmd_list))

    # ts文件上传服务器
    local_file_path = '%s/%s' % (ts_parameter.ts_file_path, ts_file_name)
    target_file_path = '%s/%s' % (server_model.server_path_dict['datafetcher_messagefile'], ts_file_name)
    server_model.upload_file(local_file_path, target_file_path)

    # 解析文件生成持仓信息
    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       '/home/trader/anaconda2/bin/python ts_position_analysis.py'
    ]
    server_model.run_cmd_str(';'.join(update_cmd_list))

    tmp_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder']]
    for account_name in ts_parameter.account_list:
        tmp_cmd_list.append('/home/trader/anaconda2/bin/python screen_tools.py -s MainFrame -c "update account %s"' % account_name)
    server_model.run_cmd_str(';'.join(tmp_cmd_list))


def __init_ts_file_dict():
    ts_file_dict = dict()
    for ts_file_folder_item in TS_FILE_FOLDER_DICT_STR.split(';'):
        account_name, ts_file_path = ts_file_folder_item.split('|')
        ts_file_dict[account_name] = ts_file_path
    return ts_file_dict


def ts_update_index(target_server_name, account_name):
    ts_file_dict = __init_ts_file_dict()

    ts_parameter1 = Ts_Parameter()
    ts_parameter1.server_name = target_server_name
    ts_parameter1.ts_file_path = ts_file_dict[account_name]
    ts_parameter1.account_name = account_name
    ts_update_tool(ts_parameter1)


if __name__ == '__main__':
    pass