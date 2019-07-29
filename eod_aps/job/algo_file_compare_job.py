# -*- coding: utf-8 -*-
# 比较多因子策略购买清单和实际持仓清单比较
import os
from itertools import islice
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.schema_jobs import StrategyAccountInfo
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.job import *

fund_names = ['steady_return', ]


def __compare_strategy_position(list1, list2):
    # 标识是否存在有差异条目
    error_flag = False
    trade_info_list = []

    message_list = []
    dict1 = dict()
    for item_info in list1:
        dict1[item_info[0]] = item_info

    dict2 = dict()
    for item_info in list2:
        dict2[item_info[0]] = item_info

    set1 = set(dict1.keys())
    set2 = set(dict2.keys())
    for key in set1 | set2:
        volume1 = 0
        if key in dict1:
            volume1 = int(dict1[key][1])

        volume2 = 0
        if key in dict2:
            volume2 = int(dict2[key][1])

        if volume1 == 0 and volume2 == 0:
            continue

        if abs(volume1 - volume2) > 100:
            error_flag = True
            instrument_db = instrument_dict[key]
            if instrument_db.inactive_date is None:
                trade_info_list.append('%s,%s' % (key, (volume2 - volume1)))
                message_list.append([key, volume1, volume2, ''])
            else:
                message_list.append([key, volume1, volume2, '(Error)'])
    html_title = 'ticker,real_volume,target_volume,inactive_date'
    html_message_list = email_utils8.list_to_html(html_title, message_list)
    return html_message_list, trade_info_list, error_flag


def __get_target_position_list(server_name, fund_name):
    i = 0
    unfind_flag = True
    while unfind_flag and i < 365:
        date_str = date_utils.get_last_day(-i)
        i += 1
        target_file_path = '%s/%s/%s_base/%s.csv' % (STOCK_SELECTION_FOLDER, server_name, date_str, fund_name)
        if os.path.exists(target_file_path):
            unfind_flag = False

    target_position_list = []
    if not unfind_flag:
        with open(target_file_path, 'rb') as fr:
            for line in islice(fr, 1, None):
                line_item = line.replace('\n', '').split(',')
                if len(line_item) < 3:
                    continue
                target_position_list.append((line_item[0], line_item[3]))
    return target_position_list


def __get_pf_position_list(server_name, fund_name, pf_account_dict):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    pf_account_db = pf_account_dict[fund_name]

    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    pf_position_list = []
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.date == now_date_str,
                                                   PfPosition.id == pf_account_db.id):
        ticker = pf_position_db.symbol
        if not ticker.isdigit():
            continue
        pf_position_list.append((ticker, pf_position_db.long))
    server_model.close()
    return pf_position_list


def algo_file_compare_job():
    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    type_list = [Instrument_Type_Enums.CommonStock, ]
    global instrument_dict
    instrument_dict = query_instrument_dict('host', type_list)

    server_host = server_constant.get_server_model('host')

    email_message_list = []
    session_jobs = server_host.get_db_session('jobs')
    query = session_jobs.query(StrategyAccountInfo)
    for strategy_account_info_db in query:
        pf_account_dict = dict()
        server_name = strategy_account_info_db.server_name
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_pf_account = session_portfolio.query(PfAccount)
        for pf_account_db in query_pf_account.filter(PfAccount.group_name == strategy_account_info_db.group_name):
            pf_account_dict[pf_account_db.fund_name] = pf_account_db

        for number_str in strategy_account_info_db.all_number.split(','):
            fund_name = '%s_%s-%s-%s-' % (strategy_account_info_db.strategy_name,
                                          number_str, strategy_account_info_db.group_name, strategy_account_info_db.fund)
            target_position_list = __get_target_position_list(server_name, fund_name)
            pf_position_list = __get_pf_position_list(server_name, fund_name, pf_account_dict)
            fund_compare_result, trade_info_list, error_flag = __compare_strategy_position(pf_position_list, target_position_list)

            if error_flag:
                email_message_list.append('<h>%s</h>' % fund_name)
                email_message_list.extend(fund_compare_result)

            if len(trade_info_list) == 0:
                continue
            classa_folder_path = '%s/%s/%s_repair' % (STOCK_SELECTION_FOLDER, server_name, now_date_str.replace('-', ''))
            if not os.path.exists(classa_folder_path):
                os.mkdir(classa_folder_path)
            file_path = '%s/%s.txt' % (classa_folder_path, fund_name)
            with open(file_path, 'w+') as fr:
                fr.write('\n'.join(trade_info_list))
        server_model.close()
    email_utils8.send_email_group_all('待交易股票列表', ''.join(email_message_list), 'html')
    server_host.close()


if __name__ == '__main__':
    algo_file_compare_job()
