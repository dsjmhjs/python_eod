# -*- coding: utf-8 -*-
# 检查history_date文件是否正常生成
import threading
from eod_aps.model.schema_common import FutureMainContract
from eod_aps.job import *


def __get_rb_maincontract():
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(FutureMainContract)
    rb_maincontract = query.filter(FutureMainContract.ticker_type == 'rb').first()
    server_host.close()
    return 'SHF%s' % rb_maincontract.main_symbol


def __history_date_check(server_name):
    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', now_date_str)

    server_model = server_constant.get_server_model(server_name)
    check_instrument = __get_rb_maincontract()
    update_cmd_list = ['cd %s/%s' % (server_model.server_path_dict['history_data_file_path'], check_instrument),
                       'tail -n 10 *.csv'
    ]
    result_str = server_model.run_cmd_str(';'.join(update_cmd_list))
    last_message = None
    for item_str in result_str.split('\n'):
        last_message = item_str
    message_str = last_message.split(',')[0]
    message_date = message_str[:10]

    validate_flag = False
    validate_time = long(date_utils.get_today_str('%H%M%S'))
    if validate_time > 153000:
        if message_date == now_date_str:
            validate_flag = True
    else:
        if message_date == last_trading_day:
            validate_flag = True

    if not validate_flag:
        error_message = 'Server:%s\nHistory Data File Date:%s' % (server_name, message_str)
        email_utils2.send_email_group_all('Build History Data File[Error]!', error_message)
    return validate_flag


def history_date_file_check_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__history_date_check, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    history_date_file_check_job(('nanhua',))
