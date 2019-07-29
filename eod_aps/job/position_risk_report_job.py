# -*- coding: utf-8 -*-
# 每日股持仓风险报告
import os
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.schema_jobs import StrategyAccountInfo
from eod_aps.job import *
from eod_aps.tools.local_server_manager import LocalServerManager

report_file_base_path = '%s/barra_report' % BARRA_REPORT_PATH


def query_strategy_position_dict(filter_date_str):
    strategy_name_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for result_item in session_jobs.query(StrategyAccountInfo.strategy_name).group_by(
            StrategyAccountInfo.strategy_name):
        strategy_name_list.append(result_item[0])
    server_host.close()

    server_position_dict = dict()
    for server_name in server_constant.get_stock_servers():
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        for query_item in session_portfolio.query(PfPosition.symbol, PfPosition.long, PfAccount.fund_name) \
                .join(PfAccount, PfPosition.id == PfAccount.id).filter(PfPosition.date == filter_date_str).all():
            symbol, volume, fund_name = query_item
            if symbol.startswith('0') or symbol.startswith('3'):
                symbol = '%s.SZ' % symbol
            elif symbol.startswith('6'):
                symbol = '%s.SH' % symbol
            else:
                continue

            key = '%s|%s' % (server_name, fund_name)
            if key in server_position_dict:
                server_position_dict[key].append((symbol, volume))
            else:
                server_position_dict[key] = [(symbol, volume)]

    strategy_position_dict = dict()
    total_position_dict = dict()
    for strategy_name in strategy_name_list:
        signal_position_dict = dict()
        for (dict_key, dict_value) in server_position_dict.items():
            if strategy_name not in dict_key:
                continue

            for dict_item in dict_value:
                symbol, volume = dict_item
                if symbol in signal_position_dict:
                    signal_position_dict[symbol] += volume
                else:
                    signal_position_dict[symbol] = volume

                if symbol in total_position_dict:
                    total_position_dict[symbol] += volume
                else:
                    total_position_dict[symbol] = volume
        strategy_position_dict[strategy_name] = signal_position_dict
    strategy_position_dict['Total'] = total_position_dict
    return strategy_position_dict


def start_r_report_program(filter_date_str):
    localserver_manager = LocalServerManager('wind_db')
    localserver_manager.barra_report(filter_date_str)


def send_report_email(strategy_list, filter_date_str):
    base_folder = '%s/%s' % (report_file_base_path, filter_date_str)
    report_file_list = ['%s/Barra_Report_%s.pdf' % (base_folder, x) for x in strategy_list]

    for check_path in report_file_list:
        if not os.path.exists(check_path):
            error_message = u"[A股持仓风险报告]文件缺失：%s!" % check_path
            raise Exception(error_message)
    # validate_time_str = date_utils.timestamp_tostring(os.path.getmtime(report_file_path), "%Y-%m-%d %H")
    # real_time_str = date_utils.get_today_str("%Y-%m-%d %H")
    # if validate_time_str != real_time_str:
    #     error_mesage = u"[A股持仓风险报告]文件生成时间异常!"
    #     raise Exception(error_mesage)
    email_utils13.send_attach_email(u'A股持仓风险报告[%s]' % filter_date_str, '', report_file_list)


def position_risk_report_job():
    filter_date_str = date_utils.get_last_trading_day('%Y-%m-%d')
    strategy_position_dict = query_strategy_position_dict(filter_date_str)
    filter_date_str = filter_date_str.replace('-', '')

    for (strategy_name, position_dict) in strategy_position_dict.items():
        if len(position_dict) == 0:
            continue

        output_list = []
        for (symbol, qty) in position_dict.items():
            output_list.append('%s,%s' % (symbol, int(qty)))
        output_list.sort()
        output_list.insert(0, 'Symbol,Position')

        position_file_path = '%s/position/%s.csv' % (BARRA_REPORT_PATH, strategy_name)
        with open(position_file_path, 'w+') as fr:
            fr.write('\n'.join(output_list))

    start_r_report_program(filter_date_str)

    strategy_list = strategy_position_dict.keys()
    send_report_email(strategy_list, filter_date_str)


if __name__ == '__main__':
    position_risk_report_job()
