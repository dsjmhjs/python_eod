# -*- coding: utf-8 -*-
# 主力合约换月
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.strategy_parameter import StrategyParameter
from eod_aps.model.trade2_history import Trade2History
from eod_aps.tools.date_utils import DateUtils
from sqlalchemy import desc
import json

fictitious_account_dict = {'All_Weather_1': '3', 'All_Weather_2': '4', 'All_Weather_3': '5', 'absolute_return': '2', 'steady_return': '3'}
contract_change_dict = {'i1705 DCE':'i1709 DCE', 'cs1705 DCE': 'cs1709 DCE', 'rb1705 SHF': 'rb1710 SHF', 'ru1705 SHF': 'ru1709 SHF', 'hc1705 SHF': 'hc1710 SHF',
                        'j1705 DCE':'j1709 DCE', 'jm1705 DCE':'jm1709 DCE', 'ni1705 SHF':'ni1709 SHF', 'l1705 DCE':'l1709 DCE', 'MA705 ZCE':'MA709 ZCE',
                        'TA705 ZCE':'TA709 ZCE', 'v1705 DCE':'v1709 DCE', 'pp1705 DCE':'pp1709 DCE', 'al1705 SHF':'al1706 SHF',
                        'SM705 ZCE':'SM709 ZCE', 'ZC705 ZCE':'ZC709 ZCE', 'IH1704':'IH1705'}
contract_change_dict2 = {'i1705':'i1709', 'cs1705': 'cs1709', 'rb1705': 'rb1710', 'ru1705': 'ru1709', 'hc1705': 'hc1710',
                         'j1705':'j1709', 'jm1705':'jm1709', 'ni1705':'ni1709', 'l1705':'l1709', 'MA705':'MA709',
                         'TA705':'TA709', 'v1705':'v1709', 'pp1705':'pp1709', 'al1705':'al1706', 'SM705':'SM709', 'ZC705':'ZC709','IH1704':'IH1705'}
date_utils = DateUtils()


def rebuild_strategy_parameter():
    server_model1 = ServerConstant().get_server_model('host')
    session_strategy1 = server_model1.get_db_session('strategy')
    query_sql = 'select a.name from strategy.strategy_online a group by a.name'
    strategy_name_list = []
    for db_item in session_strategy1.execute(query_sql):
        strategy_name_list.append(db_item[0])
    session_strategy1.commit()

    server_model = ServerConstant().get_server_model(server_name)
    session_strategy = server_model.get_db_session('strategy')

    strategy_parameter_db_list = []
    query_strategy_parameter = session_strategy.query(StrategyParameter)
    for strategy_name in strategy_name_list:
        strategy_parameter_db = query_strategy_parameter.filter(StrategyParameter.name == strategy_name).order_by(desc(StrategyParameter.time)).first()
        if strategy_parameter_db is None:
            print 'unfind %s' % strategy_name
            continue
        strategy_parameter_dict = json.loads(strategy_parameter_db.value)
        if 'Target' in strategy_parameter_dict:
            target_ticker_str = strategy_parameter_dict['Target']
            new_target_ticker_list = []
            change_flag = False
            for target_ticker in target_ticker_str.split(';'):
                if target_ticker in contract_change_dict2:
                    change_flag = True
                    change_ticker = contract_change_dict2[target_ticker]
                    new_target_ticker_list.append(change_ticker)
                else:
                    new_target_ticker_list.append(target_ticker)

            if change_flag:
                print '%s:%s ---> %s' % (strategy_name, target_ticker_str, ';'.join(new_target_ticker_list))
                strategy_parameter_dict['Target'] = ';'.join(new_target_ticker_list)
                strategy_parameter_db.value = json.dumps(strategy_parameter_dict)
                strategy_parameter_db_list.append(strategy_parameter_db)
            else:
                print '%s:%s' % (strategy_name, target_ticker_str)

    for strategy_parameter_db in strategy_parameter_db_list:
        session_strategy.merge(strategy_parameter_db)
    session_strategy.commit()



def __get_account_id_list():
    server_model = ServerConstant().get_server_model('host')
    session_strategy = server_model.get_db_session('strategy')
    query_sql = 'select a.STRATEGY_NAME from strategy.strategy_online a group by a.STRATEGY_NAME'
    strategy_name_list = []
    for db_item in session_strategy.execute(query_sql):
        strategy_name_list.append(db_item[0])

    pf_account_id_list = []
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.group_name.in_(tuple(strategy_name_list))):
        pf_account_id_list.append(int(pf_account_db.id))
    return pf_account_id_list


def main_contract_change_tools(symbol, filter_date_str):
    pf_account_id_list = __get_account_id_list()

    next_trading_day = date_utils.get_next_trading_day('%Y-%m-%d',filter_date_str)

    server_model = ServerConstant().get_server_model(server_name)
    session_om = server_model.get_db_session('om')

    pf_account_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account:
        pf_account_dict[pf_account_db.id] = pf_account_db

    instrument_db_dict = dict()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        if instrument_db.exchange_id == 20:
            exchange_name = 'SHF'
        if instrument_db.exchange_id == 21:
            exchange_name = 'DCE'
        if instrument_db.exchange_id == 22:
            exchange_name = 'ZCE'
        dict_key = '%s %s' % (instrument_db.ticker, exchange_name)
        if instrument_db.exchange_id == 25:
            dict_key = instrument_db.ticker
        instrument_db_dict[dict_key] = instrument_db

    fictitious_symbol_position_dict = dict()
    fictitious_change_symbol_position_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.symbol.like(symbol + '%'),
                                                   PfPosition.date == next_trading_day,
                                                   PfPosition.id.in_(tuple(pf_account_id_list))):
        if pf_position_db.long == 0 and pf_position_db.short == 0:
            continue

        pf_account_db = pf_account_dict[pf_position_db.id]
        if 'All_Weather_1' in pf_account_db.fund_name:
            account_name = '11610021-CTP-All_Weather_1-00'
            fictitious_account_id = fictitious_account_dict['All_Weather_1']
        elif 'All_Weather_2' in pf_account_db.fund_name:
            account_name = '11610021-CTP-All_Weather_2-00'
            fictitious_account_id = fictitious_account_dict['All_Weather_2']
        elif 'All_Weather_3' in pf_account_db.fund_name:
            account_name = '11610021-CTP-All_Weather_3-00'
            fictitious_account_id = fictitious_account_dict['All_Weather_3']
        elif 'absolute_return' in pf_account_db.fund_name:
            account_name = '120301313-CTP-absolute_return-00'
            fictitious_account_id = fictitious_account_dict['absolute_return']
        elif 'steady_return' in pf_account_db.fund_name:
            account_name = '120301312-CTP-steady_return-00'
            fictitious_account_id = fictitious_account_dict['steady_return']
        fictitious_account_db = pf_account_dict[int(fictitious_account_id)]

        symbol = pf_position_db.symbol
        instrument_db = instrument_db_dict[symbol]
        trade2_history1 = Trade2History()
        trade2_history1.time = '%s 15:00:00' % filter_date_str
        trade2_history1.symbol = symbol
        trade2_history1.price = instrument_db.close
        trade2_history1.trade_type = 3
        trade2_history1.strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        trade2_history1.account = account_name
        if pf_position_db.short > 0:
            trade2_history1.qty = pf_position_db.short
        elif pf_position_db.long > 0:
            trade2_history1.qty = -pf_position_db.long

        trade2_history2 = Trade2History()
        trade2_history2.time = '%s 15:00:00' % filter_date_str
        trade2_history2.symbol = symbol
        trade2_history2.price = instrument_db.close
        trade2_history2.trade_type = 2
        trade2_history2.strategy_id = '%s.%s' % (fictitious_account_db.group_name, fictitious_account_db.name)
        trade2_history2.account = account_name
        if pf_position_db.short > 0:
            trade2_history2.qty = -pf_position_db.short
        elif pf_position_db.long > 0:
            trade2_history2.qty = pf_position_db.long

        change_symbol = contract_change_dict[pf_position_db.symbol]
        change_instrument_db = instrument_db_dict[change_symbol]
        trade2_history3 = Trade2History()
        trade2_history3.time = '%s 15:00:00' % filter_date_str
        trade2_history3.symbol = change_symbol
        trade2_history3.price = change_instrument_db.close
        trade2_history3.trade_type = 2
        trade2_history3.strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
        trade2_history3.account = account_name
        if pf_position_db.short > 0:
            trade2_history3.qty = -pf_position_db.short
        elif pf_position_db.long > 0:
            trade2_history3.qty = pf_position_db.long

        trade2_history4 = Trade2History()
        trade2_history4.time = '%s 15:00:00' % filter_date_str
        trade2_history4.symbol = change_symbol
        trade2_history4.price = change_instrument_db.close
        trade2_history4.trade_type = 2
        trade2_history4.strategy_id = '%s.%s' % (fictitious_account_db.group_name, fictitious_account_db.name)
        trade2_history4.account = account_name
        if pf_position_db.short > 0:
            trade2_history4.qty = pf_position_db.short
        elif pf_position_db.long > 0:
            trade2_history4.qty = -pf_position_db.long

        session_om.merge(trade2_history1)
        session_om.merge(trade2_history2)
        session_om.merge(trade2_history3)
        session_om.merge(trade2_history4)

        pf_position_db2 = PfPosition()
        pf_position_db2.date = next_trading_day
        pf_position_db2.id = fictitious_account_id
        pf_position_db2.symbol = symbol
        pf_position_db2.long = pf_position_db.long
        pf_position_db2.long_cost = pf_position_db.long * instrument_db.close * instrument_db.fut_val_pt
        pf_position_db2.long_avail = pf_position_db.long_avail
        pf_position_db2.short = pf_position_db.short
        pf_position_db2.short_cost = pf_position_db.short * instrument_db.close * instrument_db.fut_val_pt
        pf_position_db2.short_avail = pf_position_db.short_avail
        pf_position_db2.yd_position_long = pf_position_db.long
        pf_position_db2.yd_long_remain = pf_position_db.long
        pf_position_db2.yd_position_short = pf_position_db.short
        pf_position_db2.yd_short_remain = pf_position_db.short
        pf_position_db2.prev_net = pf_position_db2.yd_position_long - pf_position_db2.yd_position_short

        pf_position_db3 = PfPosition()
        pf_position_db3.date = next_trading_day
        pf_position_db3.id = fictitious_account_id
        pf_position_db3.symbol = change_symbol
        if pf_position_db.short > 0:
            pf_position_db3.long = pf_position_db.short
            pf_position_db3.long_cost = pf_position_db.short * change_instrument_db.close * change_instrument_db.fut_val_pt
            pf_position_db3.long_avail = pf_position_db.short_avail
        elif pf_position_db.long > 0:
            pf_position_db3.short = pf_position_db.long
            pf_position_db3.short_cost = pf_position_db.long * change_instrument_db.close * change_instrument_db.fut_val_pt
            pf_position_db3.short_avail = pf_position_db.long_avail
        pf_position_db3.yd_position_long = pf_position_db.long
        pf_position_db3.yd_long_remain = pf_position_db.long
        pf_position_db3.yd_position_short = pf_position_db.short
        pf_position_db3.yd_short_remain = pf_position_db.short
        pf_position_db3.prev_net = pf_position_db3.yd_position_long - pf_position_db3.yd_position_short

        pf_position_db1 = pf_position_db
        pf_position_db1.symbol = change_symbol
        pf_position_db1.long = pf_position_db.long
        pf_position_db1.long_cost = pf_position_db.long * change_instrument_db.close * instrument_db.fut_val_pt
        pf_position_db1.long_avail = pf_position_db.long_avail
        pf_position_db1.short = pf_position_db.short
        pf_position_db1.short_cost = pf_position_db.short * change_instrument_db.close * instrument_db.fut_val_pt
        pf_position_db1.short_avail = pf_position_db.short_avail
        pf_position_db1.yd_position_long = pf_position_db.long
        pf_position_db1.yd_long_remain = pf_position_db.long
        pf_position_db1.yd_position_short = pf_position_db.short
        pf_position_db1.yd_short_remain = pf_position_db.short
        pf_position_db1.prev_net = pf_position_db1.yd_position_long - pf_position_db1.yd_position_short

        session_portfolio.merge(pf_position_db1)
        if fictitious_account_id in fictitious_symbol_position_dict:
            fictitious_symbol_position_dict[fictitious_account_id].append(pf_position_db2)
        else:
            fictitious_symbol_position_dict[fictitious_account_id] = [pf_position_db2]

        if fictitious_account_id in fictitious_change_symbol_position_dict:
            fictitious_change_symbol_position_dict[fictitious_account_id].append(pf_position_db3)
        else:
            fictitious_change_symbol_position_dict[fictitious_account_id] = [pf_position_db3]

    for (fictitious_account_id, fictitious_symbol_position_list) in fictitious_symbol_position_dict.items():
        pf_position_db2 = __rebuild_position(server_model, next_trading_day, fictitious_symbol_position_list)
        session_portfolio.merge(pf_position_db2)

    for (fictitious_account_id, fictitious_symbol_position_list) in fictitious_change_symbol_position_dict.items():
        pf_position_db3 = __rebuild_position(server_model, next_trading_day, fictitious_symbol_position_list)
        session_portfolio.merge(pf_position_db3)

    session_om.commit()
    session_portfolio.commit()


def __rebuild_position(server_model, trading_day_str, fictitious_symbol_position_list):
    fictitious_position = fictitious_symbol_position_list[0]

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    default_pf_position = query_pf_position.filter(PfPosition.date == trading_day_str,
                                                   PfPosition.symbol == fictitious_position.symbol,
                                                   PfPosition.id ==fictitious_position.id).first()
    if default_pf_position is not None:
        fictitious_symbol_position_list.append(default_pf_position)

    pf_position_db = PfPosition()
    pf_position_db.date = fictitious_position.date
    pf_position_db.id = fictitious_position.id
    pf_position_db.symbol = fictitious_position.symbol
    for fictitious_position in fictitious_symbol_position_list:
        pf_position_db.long += fictitious_position.long
        pf_position_db.long_cost += fictitious_position.long_cost
        pf_position_db.long_avail += fictitious_position.long_avail
        pf_position_db.short += fictitious_position.short
        pf_position_db.short_cost += fictitious_position.short_cost
        pf_position_db.short_avail += fictitious_position.short_avail

    if pf_position_db.long >= pf_position_db.short:
        pf_position_db.long = pf_position_db.long - pf_position_db.short
        pf_position_db.long_cost = pf_position_db.long_cost - pf_position_db.short_cost
        pf_position_db.long_avail = pf_position_db.long_avail - pf_position_db.short_avail
        pf_position_db.short = 0
        pf_position_db.short_cost = 0
        pf_position_db.short_avail = 0
    else:
        pf_position_db.short = pf_position_db.short - pf_position_db.long
        pf_position_db.short_cost = pf_position_db.short_cost - pf_position_db.long_cost
        pf_position_db.short_avail = pf_position_db.short_avail - pf_position_db.long_avail
        pf_position_db.long = 0
        pf_position_db.long_cost = 0
        pf_position_db.long_avail = 0
    pf_position_db.yd_position_long = pf_position_db.long
    pf_position_db.yd_long_remain = pf_position_db.long
    pf_position_db.yd_position_short = pf_position_db.short
    pf_position_db.yd_short_remain = pf_position_db.short
    pf_position_db.prev_net = pf_position_db.yd_position_long - pf_position_db.yd_position_short
    return pf_position_db


if __name__ == '__main__':
    server_name = 'nanhua'
    for ticker in ('IH1704',):
        main_contract_change_tools(ticker, '2017-04-20')
    rebuild_strategy_parameter()
