# -*- coding: utf-8 -*-
from eod_aps.job import *


def get_future_main_contract_change_info_job():
    server_model_host = server_constant.get_server_model('host')
    session_common = server_model_host.get_db_session('common')
    query_sql = "select ticker_type,pre_main_symbol,main_symbol,next_main_symbol,exchange_id from " \
                "common.future_main_contract where update_flag = 1"
    query_result = session_common.execute(query_sql)
    future_main_contract_change_info_line_list = []
    for query_line in query_result:
        future_main_contract_change_info_line = query_line[0] + ',' + query_line[1] + ',' + \
                                                query_line[2] + ',' + str(query_line[3]) + ',' + str(query_line[4])
        future_main_contract_change_info_line_list.append(future_main_contract_change_info_line)
    future_main_contract_change_info_file_path = '%s/future_main_contract_change_info_%s.csv' % \
                                            (MAIN_CONTRACT_CHANGE_FILE_FOLDER, date_utils.get_today_str('%Y-%m-%d'))

    with open(future_main_contract_change_info_file_path, 'w+') as fr:
        fr.write('\n'.join(future_main_contract_change_info_line_list))


if __name__ == '__main__':
    get_future_main_contract_change_info_job()