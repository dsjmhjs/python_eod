# -*- coding: utf-8 -*-
import time
import json
from eod_aps.job import *


def change_calendar_parameter(server_name, change_main_contract_future, new_contract_pair):
    server_model = server_constant.get_server_model(server_name)
    session_strategy = server_model.get_db_session('strategy')
    query_sql = 'select `VALUE` from strategy.strategy_parameter where `NAME` = "CalendarMA.SU" ' \
                'ORDER BY time desc limit 1'
    query_result = session_strategy.execute(query_sql)
    calendar_parameter_str = ''
    for query_line in query_result:
        calendar_parameter_str = query_line[0]
        break
    calendar_parameter_dict = json.loads(calendar_parameter_str)
    if '%s.FrontFuture' % change_main_contract_future in calendar_parameter_dict:
        calendar_parameter_dict['%s.FrontFuture' % change_main_contract_future] = new_contract_pair[0]
    if '%s.BackFuture' % change_main_contract_future in calendar_parameter_dict:
        calendar_parameter_dict['%s.BackFuture' % change_main_contract_future] = new_contract_pair[1]

    # add check instrument existance
    query_sql2 = 'select * from common.instrument where ticker = "%s";' % new_contract_pair[1]
    query_result2 = session_strategy.execute(query_sql2)
    backfuture_exist_flag = False
    if query_result2:
        backfuture_exist_flag = True

    if not backfuture_exist_flag:
        calendar_parameter_dict['%s.BackFuture' % change_main_contract_future] = ''
    calendar_parameter_dict['%s.enable' % change_main_contract_future] = 0
    calendar_parameter_str_new = json.dumps(calendar_parameter_dict)

    insert_sql_base = '''Insert Into strategy.strategy_parameter(TIME,NAME,VALUE)
                                VALUES(sysdate(),'CalendarMA.SU','%s')'''
    insert_sql = insert_sql_base % calendar_parameter_str_new
    session_strategy.execute(insert_sql)
    session_strategy.commit()
    server_model.close()


def get_change_main_contract_future():
    server_model_host = server_constant.get_server_model('host')
    session_common = server_model_host.get_db_session('common')
    query_sql = "select ticker_type, pre_main_symbol, main_symbol, next_main_symbol from " \
                "common.future_main_contract where update_flag = 1"
    query_result = session_common.execute(query_sql)
    change_month_info_list = []
    for query_line in query_result:
        change_month_info = [query_line[0], query_line[1], query_line[2], query_line[3]]
        change_month_info_list.append(change_month_info)
    server_model_host.close()
    return change_month_info_list


def main_contract_change_for_calendar_job(server_list):
    change_month_info_list = get_change_main_contract_future()
    for server_name in server_list:
        for change_month_info in change_month_info_list:
            change_main_contract_future = change_month_info[0]
            new_contract_pair = [change_month_info[2], change_month_info[3]]
            custom_log.log_error_job('Server:%s,Type:%s,Main_Ticker:%s' % (server_name, change_main_contract_future,
                                                                            new_contract_pair))
            change_calendar_parameter(server_name, change_main_contract_future, new_contract_pair)
            time.sleep(5)


if __name__ == '__main__':
    calendar_servers = server_constant.get_calendar_servers()
    main_contract_change_for_calendar_job(calendar_servers)
