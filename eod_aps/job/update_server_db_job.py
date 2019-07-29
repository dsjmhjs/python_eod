# -*- coding: utf-8 -*-
# 更新托管服务器数据库的行情或持仓
import threading
import json
import traceback

from eod_aps.model.schema_common import Instrument
from eod_aps.job import *


# 更新pcf的tradingday信息
def update_server_pcf_tradingday(server_name, trading_day=None):
    if trading_day is None:
        trading_day = date_utils.get_today_str()
    last_trading_day = date_utils.get_last_trading_day('%Y%m%d', trading_day)

    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')
    type_list = [Instrument_Type_Enums.MutualFund, Instrument_Type_Enums.MMF, Instrument_Type_Enums.StructuredFund]
    for instrument_db in session_common.query(Instrument).filter(Instrument.type_id.in_(type_list),
                                                                 Instrument.del_flag == 0):
        if instrument_db.pcf is None:
            continue
        pcf_dict = json.loads(instrument_db.pcf)
        pcf_dict['TradingDay'] = trading_day
        if 'PreTradingDay' in pcf_dict:
            pcf_dict['PreTradingDay'] = last_trading_day
        instrument_db.pcf = json.dumps(pcf_dict)
        session_common.merge(instrument_db)
    session_common.commit()


# 发现部分合约存在返回的数据close为0的情况
def __pre_update_market(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        session_common = server_model.get_db_session('common')
        update_sql = 'update common.instrument set PREV_CLOSE = `CLOSE` \
    where DEL_FLAG = 0 and INACTIVE_DATE is null and `CLOSE` != 0'
        session_common.execute(update_sql)
        session_common.commit()
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__pre_update_market:%s.' % server_name, error_msg)


def update_position_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_server_position, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def pre_update_market_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__pre_update_market, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def update_server_market_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_server_instrument, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def update_server_instrument(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_server_instrument_table, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def update_server_etf_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_server_etf, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


def rebuild_server_market_files_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__rebuild_instrument_files, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


# 刪除之前生成或上传的文件
def __del_pre_files(server_model):
    del_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                    'rm -f `ls *%s*|grep -v CTP_Market`' % date_utils.get_today_str('%Y-%m-%d')
                    ]
    server_model.run_cmd_str(';'.join(del_cmd_list))


# 备份新生成的文件
def __backup_files(server_model):
    date_str_1 = date_utils.get_today_str('%Y-%m-%d')
    date_str_2 = date_utils.get_today_str('%Y%m%d%H%M')
    backup_file_path = '%s/%s' % (server_model.server_path_dict['datafetcher_messagefile_backup'], date_str_2)
    backup_cmd_list = ['mkdir %s' % backup_file_path,
                       'cd %s' % server_model.server_path_dict['datafetcher_messagefile'],
                       'cp *%s* %s' % (date_str_1, backup_file_path)
                       ]
    server_model.run_cmd_str(';'.join(backup_cmd_list))


# 生成持仓数据文件
def __build_position_files(server_model):
    position_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_project_folder'],
                         './build64_release/fetcher/fetch_position'
                         ]
    server_model.run_cmd_str(';'.join(position_cmd_list))


# 生成持仓数据文件:从华宝的Lts生成股票，期权等的行情，南华的CTP生成期货的行情
def __build_instrument_files(server_model):
    if server_model.data_source_type != '':
        instrument_cmd_list = ['cd %s' % server_model.server_path_dict['datafetcher_project_folder'],
                               './build64_release/fetcher/fetch_instrument -a %s' % server_model.data_source_type
                               ]
        server_model.run_cmd_str(';'.join(instrument_cmd_list))
        __validate_instrument_log(server_model)


# 检验日志输出，是否有部分数据未收到行情
def __validate_instrument_log(server_model):
    filter_date_str = date_utils.get_today_str('%Y%m%d')
    log_base_path = '%s/log' % server_model.server_path_dict['datafetcher_project_folder']
    cmd_list = ['cd %s' % log_base_path,
                'ls *fetch_instrument_%s*.log' % filter_date_str
                ]
    cmd_return_str = server_model.run_cmd_str(';'.join(cmd_list))
    log_file_list = []
    for log_file_name in cmd_return_str.split('\n'):
        if len(log_file_name) > 0:
            log_file_list.append(log_file_name)
    log_file_list.sort()

    if len(log_file_list) == 0:
        return

    log_file_name = log_file_list[-1]
    cmd_list = ['cd %s' % log_base_path,
                'tail -100 %s' % log_file_name
                ]
    cmd_return_str = server_model.run_cmd_str(';'.join(cmd_list))
    if 'Login fail' in cmd_return_str:
        email_content = "<div style='background-color: #ee4c50'>%s</div>" % cmd_return_str.replace('\n', '<br>')
        email_utils2.send_email_group_all('[Error]fetch_instrument has error!', email_content, 'html')


def __update_position_cmd(server_model):
    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       '/home/trader/anaconda2/bin/python Lts_position_analysis.py',
                       '/home/trader/anaconda2/bin/python guosen_position_analysis.py',
                       '/home/trader/anaconda2/bin/python ctp_position_analysis.py',
                       '/home/trader/anaconda2/bin/python xt_position_analysis.py',
                       '/home/trader/anaconda2/bin/python rebuild_trade_calculation_position.py'
                       ]
    server_model.run_cmd_str(';'.join(update_cmd_list))


def __update_instrument_cmd(server_model):
    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       '/home/trader/anaconda2/bin/python ctp_price_analysis.py',
                       '/home/trader/anaconda2/bin/python Lts_price_analysis.py'
                       ]
    server_model.run_cmd_str(';'.join(update_cmd_list))


def __update_instrument_new_cmd(server_model):
    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       '/home/trader/anaconda2/bin/python update_instrument.py',
                       ]
    server_model.run_cmd_str(';'.join(update_cmd_list))


def __update_etf_cmd(server_model):
    update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                       '/home/trader/anaconda2/bin/python update_by_etf_file.py'
                       ]
    server_model.run_cmd_str(';'.join(update_cmd_list))


# 同时从券商接口处获取持仓和行情文件
def __build_position_instrument_files(server_model):
    __del_pre_files(server_model)
    __build_position_files(server_model)
    # __build_instrument_files(server_model)
    __backup_files(server_model)


# 重新生成行情文件
def __rebuild_instrument_files(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        __build_instrument_files(server_model)
        __backup_files(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__rebuild_instrument_files:%s.' % server_name, error_msg)


# 根据数据文件更新持仓
def __update_server_position(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        __build_position_instrument_files(server_model)
        __update_position_cmd(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_server_position:%s.' % server_name, error_msg)


# 根据数据文件更新行情
def __update_server_instrument(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        __update_instrument_cmd(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_server_instrument:%s.' % server_name, error_msg)


def __update_server_instrument_table(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        __update_instrument_new_cmd(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_server_instrument_table:%s.' % server_name, error_msg)


# 根据数据文件更新ETF数据
def __update_server_etf(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        __update_etf_cmd(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_server_etf:%s.' % server_name, error_msg)


if __name__ == '__main__':
    __update_server_position('huabao')
