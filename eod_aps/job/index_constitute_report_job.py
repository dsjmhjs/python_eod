# -*- coding: utf-8 -*-
# 生成账户层面的股票仓位分指数构成报告
from eod_aps.model.schema_common import FutureMainContract
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.tools.instrument_tools import query_instrument_dict
from eod_aps.tools.performance_calculation_tools import *
from eod_aps.job import *


def __build_main_contract_dict():
    session_common = server_host.get_db_session('common')
    query = session_common.query(FutureMainContract)
    return {x.ticker_type: x for x in query}


def query_fund_dict(server_list, date_str):
    fund_dict = dict()
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')

        pf_account_id_dict = {x.id: x for x in session_portfolio.query(PfAccount)}

        query_position = session_portfolio.query(PfPosition)
        for position_db in query_position.filter(PfPosition.date == date_str):
            if position_db.symbol not in instrument_db_dict:
                continue
            instrument_db = instrument_db_dict[position_db.symbol]
            if not(instrument_db.type_id == Instrument_Type_Enums.CommonStock or
                   (instrument_db.type_id == Instrument_Type_Enums.Future and ('IC' in instrument_db.ticker or 'IF' in instrument_db.ticker))):
                continue

            position_view = Position_View(position_db)
            pf_account_db = pf_account_id_dict[position_db.id]
            fund_name = pf_account_db.fund_name.split('-')[2]
            if fund_name in fund_dict:
                group_dict = fund_dict[fund_name]
                if pf_account_db.group_name in group_dict:
                    group_dict[pf_account_db.group_name].append(position_view)
                else:
                    group_dict[pf_account_db.group_name] = [position_view]
            else:
                group_dict = dict()
                group_dict[pf_account_db.group_name] = [position_view]
                fund_dict[fund_name] = group_dict
    return fund_dict


def daily_return_calculation(fund_dict, date_str):
    pre_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)
    main_contract_dict = __build_main_contract_dict()
    with StockWindUtils() as stock_wind_utils:
        ticker_type_list = [const.INSTRUMENT_TYPE_ENUMS.CommonStock, const.INSTRUMENT_TYPE_ENUMS.Future]
        common_ticker_list = stock_wind_utils.get_ticker_list(ticker_type_list)
        close_dict = stock_wind_utils.get_close_dict(date_str, common_ticker_list)

    out_put_list = []
    for (fund_name, group_dict) in fund_dict.items():
        for (group_name, positio_list) in group_dict.items():
            ic_number = 0
            if_number = 0
            for position_info_db in positio_list:
                if 'IC' in position_info_db.symbol:
                    ic_number += position_info_db.long - position_info_db.short
                elif 'IF' in position_info_db.symbol:
                    if_number += position_info_db.long - position_info_db.short

            performance_calculation = PerformanceCalculation((pre_date_str, []), (date_str, positio_list), [])
            performance_calculation.set_instrument_db_dict(instrument_db_dict)
            performance_calculation.set_close_dict(close_dict)
            stock_value_total, hedge_value_total, csi300_value_total, zz500_value_total = performance_calculation.position_makeup_report()

            net_value = stock_value_total + hedge_value_total
            other_value = stock_value_total - csi300_value_total - zz500_value_total

            if stock_value_total > 0:
                csi300_weight = csi300_value_total / stock_value_total * 100
                zz500_weight = zz500_value_total / stock_value_total * 100
                other_weight = other_value / stock_value_total * 100
            else:
                csi300_weight = 0
                zz500_weight = 0
                other_weight = 0

            main_contract_ic = main_contract_dict['IC']
            instrument_ic = instrument_db_dict[main_contract_ic.main_symbol]
            zz500_ic = -__round_down(zz500_value_total / (instrument_ic.close * instrument_ic.fut_val_pt))
            other_ic = -__round_down(other_value / (instrument_ic.close * instrument_ic.fut_val_pt))

            main_contract_if = main_contract_dict['IF']
            main_contract_if = instrument_db_dict[main_contract_if.main_symbol]
            csi300_if = -__round_down(csi300_value_total / (main_contract_if.close * main_contract_if.fut_val_pt))

            ic_diff = ic_number - (zz500_ic + other_ic)
            if_diff = if_number - csi300_if
            out_put_str = '%s,%s,%s,%.f,%.f,%.f,%.f,%.f,%.f,%.f%%,%.2f%%,%.2f%%,%s,%s,%s,%s,%s,%s,%s' % \
('', fund_name, group_name, stock_value_total, hedge_value_total, net_value, csi300_value_total,
 zz500_value_total, other_value, csi300_weight, zz500_weight, other_weight, if_number, ic_number, csi300_if,
 zz500_ic, other_ic, if_diff, ic_diff)
            out_put_list.append(out_put_str)
    return out_put_list


# 向下取整
def __round_down(number_input):
    return int(float(number_input))


def __build_collection_html(report_message_list):
    report_message_dict = dict()
    for report_message_info in report_message_list:
        report_message_items = report_message_info.split(',')
        if report_message_items[1] in report_message_dict:
            report_message_dict[report_message_items[1]].append(report_message_info)
        else:
            report_message_dict[report_message_items[1]] = [report_message_info]

    collection_list = []
    for (fund_name, temp_report_message_list) in report_message_dict.items():
        stock_value_total = 0.0
        hedge_value_total = 0.0
        net_value = 0.0
        if_value = 0.0
        ic_value = 0.0
        csi300_if_value = 0.0
        zz500_ic_value = 0.0
        for report_message_info in temp_report_message_list:
            report_message_items = report_message_info.split(',')
            stock_value_total += float(report_message_items[3])
            hedge_value_total += float(report_message_items[4])
            net_value += float(report_message_items[5])

            if_value += float(report_message_items[12])
            ic_value += float(report_message_items[13])
            csi300_if_value += float(report_message_items[14])
            zz500_ic_value += float(report_message_items[15])
        collection_list.append([fund_name, '%.f' % stock_value_total, '%.f' % hedge_value_total, '%.f' % net_value,
                                '%.f' % if_value, '%.f' % ic_value, '%.f' % csi300_if_value, '%.f' % zz500_ic_value])
    collection_title = 'fund_name,stock_value_total,hedge_value_total,net_value,IF actual,IC actual,CSI300_IF,ZZ500_IC'
    html_list = email_utils4.list_to_html(collection_title, collection_list)
    return ''.join(html_list)


def index_constitute_report_job(server_list, date_str=None):
    if date_str is None:
        date_str = date_utils.get_next_trading_day('%Y-%m-%d')
    global server_host, instrument_db_dict
    server_host = server_constant.get_server_model('host')
    instrument_db_dict = query_instrument_dict('host')

    fund_dict = query_fund_dict(server_list, date_str)
    report_message_list = daily_return_calculation(fund_dict, date_str)

    report_message_list.sort()
    html_list = __build_collection_html(report_message_list)

    title = 'server_name,fund_name,strategy_group_name,stock_value_total,hedge_value_total,net_value,\
CSI300_value,ZZ500_value,Other_value,CSI300_weight,ZZ500_weight,Other_weight,IF,IC,CSI300_IF,ZZ500_IC,\
Other_IC,IF_diff,IC_idff'
    report_message_list.insert(0, title)

    file_save_path = '%s/log/report.csv' % EOD_PROJECT_FOLDER
    with open(file_save_path, 'w') as fr:
        fr.write('\n'.join(report_message_list))

    email_utils4.send_email_path(u'股票仓位分指数构成报告', html_list, file_save_path, 'html')
    server_host.close()


if __name__ == '__main__':
    all_trade_servers_list = server_constant.get_all_trade_servers()
    index_constitute_report_job(all_trade_servers_list, '2018-04-12')
