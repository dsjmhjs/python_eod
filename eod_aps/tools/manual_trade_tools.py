# -*- coding: utf-8 -*-
from decimal import Decimal
from eod_aps.model.schema_common import Instrument
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.server_constans import server_constant
from eod_aps.model.schema_jobs import StrategyAccountInfo
from eod_aps.model.schema_om import Trade2History
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.stock_utils import StockUtils

date_utils = DateUtils()
stock_utils = StockUtils()

instrument_dict = dict()


class ManualTrade(object):
    """
        主力合约
    """
    symbol = ''
    # >0:long  <0:short
    qty = 0
    price = 0.0
    # 2:open  3:close
    trade_type = ''
    date_str = ''

    def __init__(self):
        pass


def __add_trade2_history(server_model, pf_account_db, manual_trade):
    session_om = server_model.get_db_session('om')
    query_sql = 'select max(id) from om.trade2_history'
    max_id = session_om.execute(query_sql).first()[0]

    trade2_history = Trade2History()
    trade2_history.id = max_id + 1
    trade2_history.time = '%s 15:30:00' % manual_trade.date_str
    trade2_history.symbol = '%s CFF' % manual_trade.symbol
    trade2_history.qty = manual_trade.qty
    trade2_history.price = manual_trade.price
    trade2_history.trade_type = manual_trade.trade_type
    trade2_history.strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
    if 'xhms01' in pf_account_db.fund_name:
        trade2_history.account = '10180357-PROXY-xhms01-'
    elif 'xhms02' in pf_account_db.fund_name:
        trade2_history.account = '10180356-PROXY-xhhm02-'
    elif 'balance01' in pf_account_db.fund_name:
        trade2_history.account = '198800888042-TS-balance01-'
    else:
        trade2_history.account = '060000006182-HUABAO-steady_return-01'

    trade2_history.hedgeflag = 0
    trade2_history.self_cross = 0

    session_om.merge(trade2_history)
    session_om.commit()


def __add_pf_position(server_model, pf_account_db, manual_trade):
    instrument_dict = dict()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        instrument_dict[instrument_db.ticker] = instrument_db

    instrument_db = instrument_dict[manual_trade.symbol]

    start_date = date_utils.string_toDatetime(manual_trade.date_str)
    session_portfolio = server_model.get_db_session('portfolio')
    result_item = session_portfolio.execute('select max(date) from portfolio.pf_position t').first()
    end_date = date_utils.string_toDatetime(str(result_item[0]))
    trading_day_list = date_utils.get_trading_day_list(start_date, end_date)

    for trading_day_str in trading_day_list:
        pf_position = PfPosition()
        pf_position.date = trading_day_str
        pf_position.id = pf_account_db.id
        pf_position.symbol = manual_trade.symbol
        pf_position.hedgeflag = 0

        if manual_trade.qty > 0:
            pf_position.long = manual_trade.qty
            pf_position.long_cost = Decimal(manual_trade.qty) * Decimal(manual_trade.price) * instrument_db.fut_val_pt
            pf_position.long_avail = manual_trade.qty
            pf_position.yd_position_long = manual_trade.qty
            pf_position.yd_long_remain = manual_trade.qty
        else:
            pf_position.short = abs(manual_trade.qty)
            pf_position.short_cost = Decimal(abs(manual_trade.qty)) * Decimal(manual_trade.price) * instrument_db.fut_val_pt
            pf_position.short_avail = abs(manual_trade.qty)
            pf_position.yd_position_short = abs(manual_trade.qty)
            pf_position.yd_short_remain = abs(manual_trade.qty)
        pf_position.prev_net = pf_position.yd_position_long - pf_position.yd_position_short
        session_portfolio.merge(pf_position)
    session_portfolio.commit()


def __del_pf_position(server_model, pf_account_db, manual_trade):
    session_portfolio = server_model.get_db_session('portfolio')
    query_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_position.filter(PfPosition.id == pf_account_db.id,
                                                PfPosition.symbol == manual_trade.symbol,
                                                PfPosition.date >= manual_trade.date_str):
        session_portfolio.delete(pf_position_db)
    session_portfolio.commit()


def add_manualtrade_huabao(manual_trade):
    server_model = server_constant.get_server_model(server_name)

    pf_account_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        pf_account_dict[pf_account_db.fund_name] = pf_account_db

    if manual_trade.strategy_id not in pf_account_dict:
        print 'error strategy_id:%s' % manual_trade.strategy_id
        return

    pf_account_db = pf_account_dict[manual_trade.strategy_id]
    __add_trade2_history(server_model, pf_account_db, manual_trade)

    if manual_trade.trade_type == 2:
        __add_pf_position(server_model, pf_account_db, manual_trade)
    elif manual_trade.trade_type == 3:
        __del_pf_position(server_model, pf_account_db, manual_trade)


# 多因子策略策略账号
def __query_intraday_fund_list(server_name):
    intraday_fund_list = []
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    query = session_jobs.query(StrategyAccountInfo)
    for strategyaccount_info_db in query.filter(StrategyAccountInfo.server_name == server_name):
        for number in strategyaccount_info_db.all_number.split(','):
            fund_name = '%s_%s-%s-%s-' % \
                        (strategyaccount_info_db.strategy_name,
                         number,
                         strategyaccount_info_db.group_name, strategyaccount_info_db.fund)
            intraday_fund_list.append(fund_name)
    return intraday_fund_list


# 整体切换所有篮子的主力合约
def __change_main_contract(trading_day, fund_name):
    pass


def change_main_contract(start_date_str, pre_symbol, change_symbol):
    intraday_fund_list = __query_intraday_fund_list(server_name)
    __build_instrument_dict()

    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)

    pf_account_id_list = []
    pf_account_dict = dict()
    for pf_account_db in query_pf_account:
        # if pf_account_db.fund_name in intraday_fund_list:
        pf_account_id_list.append(pf_account_db.id)
        pf_account_dict[pf_account_db.id] = pf_account_db

    query_position = session_portfolio.query(PfPosition)
    pre_symbol_position_dict = dict()
    for pf_position_db in query_position.filter(PfPosition.symbol == pre_symbol,
                                                PfPosition.date >= start_date_str).order_by(PfPosition.date):
        if pf_position_db.id in pre_symbol_position_dict:
            pre_symbol_position_dict[pf_position_db.id].append(pf_position_db)
        else:
            pre_symbol_position_dict[pf_position_db.id] = [pf_position_db]

    change_symbol_position_dict = dict()
    for pf_position_db in query_position.filter(PfPosition.symbol == change_symbol,
                                                PfPosition.date >= start_date_str).order_by(PfPosition.date):
        dict_key = '%s|%s' % (pf_position_db.id, pf_position_db.date.strftime('%Y-%m-%d'))
        change_symbol_position_dict[dict_key] = pf_position_db

    change_instrument_db = instrument_dict[change_symbol]
    for pf_account_id in pf_account_id_list:
        if pf_account_id not in pre_symbol_position_dict:
            continue
        pf_position_list = pre_symbol_position_dict[pf_account_id]
        cost_money = 0
        for pf_position_db in pf_position_list:
            date_str = pf_position_db.date.strftime('%Y-%m-%d')
            # 增加交易记录
            if date_str == start_date_str:
                prev_close1 = stock_utils.get_prev_close(date_str.replace('-', ''), pre_symbol)
                manual_trade1 = ManualTrade()
                manual_trade1.symbol = pre_symbol
                if pf_position_db.long > 0:
                    manual_trade1.qty = -pf_position_db.long
                elif pf_position_db.short > 0:
                    manual_trade1.qty = pf_position_db.short
                manual_trade1.price = prev_close1
                manual_trade1.trade_type = 3
                manual_trade1.date_str = date_str

                prev_close2 = stock_utils.get_prev_close(date_str.replace('-', ''), change_symbol)
                manual_trade2 = ManualTrade()
                manual_trade2.symbol = change_symbol
                if pf_position_db.long > 0:
                    manual_trade2.qty = pf_position_db.long
                    cost_money = Decimal(pf_position_db.long) * Decimal(prev_close2) * change_instrument_db.fut_val_pt
                elif pf_position_db.short > 0:
                    manual_trade2.qty = -pf_position_db.short
                    cost_money = Decimal(pf_position_db.long) * Decimal(prev_close2) * change_instrument_db.fut_val_pt
                manual_trade2.price = prev_close2
                manual_trade2.trade_type = 2
                manual_trade2.date_str = date_str

                pf_account_db = pf_account_dict[pf_position_db.id]
                __add_trade2_history(server_model, pf_account_db, manual_trade1)
                __add_trade2_history(server_model, pf_account_db, manual_trade2)

            dict_key = '%s|%s' % (pf_position_db.id, date_str)
            if dict_key in change_symbol_position_dict:
                change_pf_position_db = change_symbol_position_dict[dict_key]
                change_pf_position_db.long += pf_position_db.long
                change_pf_position_db.long_cost += cost_money
                change_pf_position_db.long_avail += pf_position_db.long_avail
                change_pf_position_db.short += pf_position_db.short
                change_pf_position_db.short_cost += cost_money
                change_pf_position_db.short_avail += pf_position_db.short_avail

                change_pf_position_db.yd_position_long += pf_position_db.yd_position_long
                change_pf_position_db.yd_long_remain += pf_position_db.yd_long_remain
                change_pf_position_db.yd_position_short += pf_position_db.yd_position_short
                change_pf_position_db.yd_short_remain += pf_position_db.yd_short_remain
                change_pf_position_db.prev_net = change_pf_position_db.yd_position_long - change_pf_position_db.yd_position_short
                session_portfolio.delete(change_pf_position_db)
                session_portfolio.delete(pf_position_db)
            else:
                pf_position_db.symbol = change_symbol
                if pf_position_db.long > 0:
                    manual_trade1.long_cost = cost_money
                elif pf_position_db.short > 0:
                    manual_trade1.short_cost = cost_money
                session_portfolio.merge(pf_position_db)
    session_portfolio.commit()


def __build_instrument_dict():
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        instrument_dict[instrument_db.ticker] = instrument_db


if __name__ == '__main__':
    server_name = 'huabao'
    change_main_contract('2017-08-14', 'IF1708', 'IF1709')