# -*- coding: utf-8 -*-
from eod_aps.model.future_main_contract import FutureMainContract
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.tools.performance_calculation_tools import *
date_utils = DateUtils()

def daily_return_calculation(server_name, pf_account_id, date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')
    pre_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)

    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')

    pf_account_dict = dict()
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        pf_account_dict[pf_account_db.id] = pf_account_db

    pre_positio_list = []
    query_position = session_portfolio.query(PfPosition)
    for position_db in query_position.filter(PfPosition.id == pf_account_id, PfPosition.date == pre_date_str):
        position_view = Position_View(position_db)
        pre_positio_list.append(position_view)

    positio_list = []
    for position_db in query_position.filter(PfPosition.id == pf_account_id, PfPosition.date == date_str):
        position_view = Position_View(position_db)
        positio_list.append(position_view)

    trade_list = []
    pf_account_db = pf_account_dict[pf_account_id]
    strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
    account_name = pf_account_db.fund_name.split('-')[2]

    query_trade = session_om.query(Trade2History)
    for trade_db in query_trade.filter(Trade2History.strategy_id == strategy_id, Trade2History.account.like('%' + account_name + '%'), Trade2History.time.like('%' + date_str + '%')):
        trade_view = Trade_View(trade_db)
        trade_list.append(trade_view)

    performance_calculation = PerformanceCalculation((pre_date_str, pre_positio_list), (date_str, positio_list), trade_list)
    print performance_calculation.performance_calculation()


def daily_return_calculation2(server_name, fund_name, date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')
    pre_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)

    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')

    pf_account_list = []
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        if  pf_account_db.fund_name == fund_name:
            pf_account_list.append(pf_account_db.id)

    # pre_positio_list = []
    # query_position = session_portfolio.query(PfPosition)
    # for position_db in query_position.filter(PfPosition.id.in_(tuple(pf_account_list),), PfPosition.date == pre_date_str):
    #     position_view = Position_View(position_db)
    #     pre_positio_list.append(position_view)

    positio_list = []
    query_position = session_portfolio.query(PfPosition)
    for position_db in query_position.filter(PfPosition.id.in_(tuple(pf_account_list),), PfPosition.date == date_str):
        position_view = Position_View(position_db)
        positio_list.append(position_view)

    fund_name_items = fund_name.split('-')
    strategy_name = '%s.%s' % (fund_name_items[1], fund_name_items[0])
    account_name = fund_name_items[2]

    trade_list = []
    query_trade = session_om.query(Trade2History)
    for trade_db in query_trade.filter(Trade2History.strategy_id.like('%' + strategy_name + '%'), Trade2History.account.like('%' + account_name + '%'), Trade2History.time.like('%' + date_str + '%')):
        trade_view = Trade_View(trade_db)
        trade_list.append(trade_view)

    performance_calculation = PerformanceCalculation((pre_date_str, []), (date_str, positio_list), trade_list)
    buy_money, sell_money, stock_value_total, hedge_value_total, position_pnl, trade_pnl = performance_calculation.report_index()
    print '%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f' % (fund_name, buy_money, sell_money, stock_value_total, hedge_value_total, position_pnl, trade_pnl)


def daily_return_calculation3(server_name, date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')
    pre_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)

    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')

    pf_account_id_dict = dict()
    pf_account_fund_dict = dict()
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        # if pf_account_db.group_name != 'MultiFactor':
        #     continue
        # if 'steady_return' in pf_account_db.fund_name:
        #     pf_account_id_dict[pf_account_db.id] = pf_account_db
        #     pf_account_fund_dict[pf_account_db.fund_name] = pf_account_db
        if 'xhms01' in pf_account_db.fund_name or 'xhms02' in pf_account_db.fund_name or 'balance01' in pf_account_db.fund_name:
            pf_account_id_dict[pf_account_db.id] = pf_account_db
            pf_account_fund_dict[pf_account_db.fund_name] = pf_account_db

    positio_dict = dict()
    query_position = session_portfolio.query(PfPosition)
    for position_db in query_position.filter(PfPosition.date == date_str):
        if position_db.id not in pf_account_id_dict:
            continue

        position_view = Position_View(position_db)
        if position_db.id in positio_dict:
            positio_dict[position_db.id].append(position_view)
        else:
            positio_dict[position_db.id] = [position_view]

    trade_dict = dict()
    query_trade = session_om.query(Trade2History)
    for trade_db in query_trade.filter(Trade2History.time.like('%' + date_str + '%')):
        strategy_id_items = trade_db.strategy_id.split('.')
        account_items = trade_db.account.split('-')
        fund_name = '%s-%s-%s-' % (strategy_id_items[1], strategy_id_items[0], account_items[2])
        if fund_name not in pf_account_fund_dict:
            continue

        pf_account_db = pf_account_fund_dict[fund_name]
        trade_view = Trade_View(trade_db)
        if pf_account_db.id in trade_dict:
            trade_dict[pf_account_db.id].append(trade_view)
        else:
            trade_dict[pf_account_db.id] = [trade_view]

    for (pf_account_id, positio_list) in positio_dict.items():
        pf_account_db = pf_account_id_dict[pf_account_id]
        if pf_account_id in trade_dict:
            trade_list = trade_dict[pf_account_id]
        else:
            trade_list = []
        performance_calculation = PerformanceCalculation((pre_date_str, []), (date_str, positio_list), trade_list)
        buy_money, sell_money, stock_value_total, hedge_value_total, position_pnl, trade_pnl = performance_calculation.report_index()
        print '%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f' % (pf_account_db.fund_name, buy_money, sell_money, stock_value_total, hedge_value_total, position_pnl, trade_pnl)


def __build_instrument_db_dict():
    instrument_db_dict = dict()
    server_model = ServerConstant().get_server_model('host')
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query:
        instrument_db_dict[instrument_db.ticker] = instrument_db
    return  instrument_db_dict

def __build_main_contract_dict():
    main_contract_dict = dict()
    server_model = ServerConstant().get_server_model('host')
    session_common = server_model.get_db_session('common')
    query = session_common.query(FutureMainContract)
    for future_maincontract_db in query:
        main_contract_dict[future_maincontract_db.ticker_type] = future_maincontract_db
    return main_contract_dict

def daily_return_calculation4(server_name, date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')
    pre_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)

    instrument_db_dict = __build_instrument_db_dict()
    main_contract_dict = __build_main_contract_dict()
    with StockWindUtils() as stock_utils:
        close_dict = stock_utils.get_close_dict(date_str)

    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    pf_account_id_dict = dict()
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        pf_account_id_dict[pf_account_db.id] = pf_account_db


    fund_dict = dict()
    query_position = session_portfolio.query(PfPosition)
    for position_db in query_position.filter(PfPosition.date == date_str):
        if position_db.symbol not in instrument_db_dict:
            print 'ticker_Error:', position_db.symbol
            continue
        instrument_db = instrument_db_dict[position_db.symbol]
        if instrument_db.type_id not in (1, 4):
            print 'type_Error:', position_db.symbol
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
            zz500_ic = -__rounding_number(zz500_value_total / (instrument_ic.close * instrument_ic.fut_val_pt))
            other_ic = -__rounding_number(other_value / (instrument_ic.close * instrument_ic.fut_val_pt))

            main_contract_if = main_contract_dict['IF']
            main_contract_if = instrument_db_dict[main_contract_if.main_symbol]
            csi300_if = -__rounding_number(csi300_value_total / (main_contract_if.close * main_contract_if.fut_val_pt))

            ic_diff = ic_number - (zz500_ic + other_ic)
            if_diff = if_number - csi300_if
            out_put_str = '%s,%s,%s,%.f,%.f,%.f,%.f,%.f,%.f,%.f%%,%.2f%%,%.2f%%,%s,%s,%s,%s,%s,%s,%s' % \
    (server_name, fund_name, group_name, stock_value_total, hedge_value_total, net_value, csi300_value_total, zz500_value_total, \
     other_value, csi300_weight, zz500_weight, other_weight, if_number, ic_number, csi300_if,  zz500_ic, other_ic, if_diff, ic_diff)
            out_put_list.append(out_put_str)
    return out_put_list


# 对数字进行四舍五入
def __rounding_number(number_input):
    return int(round(number_input, 0))


def test1():
    server_model = ServerConstant().get_server_model('huabao')
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')
    pf_account_list = []
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        if pf_account_db.group_name != 'MultiFactor':
            continue
        if 'steady_return' in pf_account_db.fund_name:
            daily_return_calculation2('huabao',  pf_account_db.fund_name, '2017-08-07')


def test2():
    daily_return_calculation3('guoxin', '2017-08-07')


def test3():
    email_utils = EmailUtils(EmailUtils.group4)
    date_str = '2017-08-09'
    out_put_list = []
    for server_name in ('huabao', 'guoxin'):
        out_put_list.extend(daily_return_calculation4(server_name, date_str))

    title = 'server_name,fund_name,strategy_group_name,stock_value_total,hedge_value_total,net_value,CSI300_value,ZZ500_value,Other_value,CSI300_weight,ZZ500_weight,Other_weight,IF,IC,CSI300_IF,ZZ500_IC,Other_IC,IF_diff,IC_idff'
    out_put_list.insert(0, title)

    save_file_path = 'E:/report.csv'
    with open(save_file_path, 'w') as fr:
        fr.write('\n'.join(out_put_list))
    email_utils.send_email_path('股票仓位分指数构成报告_%s' % date_str, '', save_file_path, 'html')

if __name__ == '__main__':
    test3()

