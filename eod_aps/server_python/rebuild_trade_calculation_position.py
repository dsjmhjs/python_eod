# -*- coding: utf-8 -*-
# 将各接口返回的tradetype类型转换成系统使用的(保存版本，根据交易所的返回trade来计算)
import json
from decimal import Decimal
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition
from eod_aps.model.schema_common import Instrument_All, InstrumentCommissionRate
from eod_aps.model.schema_om import TradeBroker
from eod_aps.server_python import *

filter_date_str = date_utils.get_today_str('%Y-%m-%d')
instrument_all_dict = dict()
instrument_commission_rate_dict = dict()
# NORMAL = 0,
# SHORT = 1,
# OPEN = 2,
# CLOSE = 3,
# CLOSE_YESTERDAY = 4,
# RedPur = 5,
# MergeSplit = 6,
# NA = 7,
# EXERCISE = 8


def __build_instrument_dict():
    query = session_common.query(Instrument_All)
    for instrument_all_db in query:
        instrument_all_dict[instrument_all_db.ticker] = instrument_all_db

    query = session_common.query(InstrumentCommissionRate)
    for icr_db in query:
        if icr_db.ticker_type == 'SHSZ300':
            ticker_type = 'IF'
        elif icr_db.ticker_type == 'SH000905':
            ticker_type = 'IC'
        elif icr_db.ticker_type == 'SSE50':
            ticker_type = 'IH'
        else:
            ticker_type = icr_db.ticker_type
        instrument_commission_rate_dict[ticker_type] = icr_db


def __calculation_position_femas(account_db, trade_list):
    position_dict = dict()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id == account_db.accountid, AccountPosition.date == filter_date_str):
        key = '%s|%s' % (position_db.symbol, position_db.hedgeflag)
        position_dict[key] = position_db

    trade_list = sorted(trade_list, cmp=lambda x, y: cmp(x.time, y.time))
    for trade_db in trade_list:
        instrument_all_db = instrument_all_dict[trade_db.symbol]
        dict_key = '%s|%s' % (trade_db.symbol, trade_db.hedgeflag)
        if dict_key not in position_dict:
            print 'error trade:', trade_db.print_info()
            continue
        position_db = position_dict[dict_key]
        qty = abs(int(trade_db.qty))
        if trade_db.qty > 0:  # 买
            position_db.day_long += qty
            position_db.day_long_cost = float(position_db.day_long_cost) + \
                                        qty * float(trade_db.price) * float(instrument_all_db.fut_val_pt)
        elif trade_db.qty < 0:  # 卖
            position_db.day_short += abs(qty)
            position_db.day_short_cost = float(position_db.day_short_cost) + \
                                         abs(qty) * float(trade_db.price) * float(instrument_all_db.fut_val_pt)

    for (symbol, position_db) in position_dict.items():
        session_portfolio.merge(position_db)


def __calculation_position_ctp(account_db, trade_list):
    position_dict = dict()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id == account_db.accountid, AccountPosition.date == filter_date_str):
        key = '%s|%s' % (position_db.symbol, position_db.hedgeflag)
        position_db.fee = 0
        position_dict[key] = position_db

    trade_list = sorted(trade_list, cmp=lambda x, y: cmp(x.time, y.time))
    for trade_info in trade_list:
        instrument_all_db = instrument_all_dict[trade_info.symbol]
        ticker_type = filter(lambda x: not x.isdigit(), trade_info.symbol)
        if ticker_type not in instrument_commission_rate_dict:
            continue
        icr_db = instrument_commission_rate_dict[ticker_type]

        dict_key = '%s|%s' % (trade_info.symbol, trade_info.hedgeflag)
        if dict_key not in position_dict:
            print 'error trade:', trade_info.print_info()
            continue
        position_db = position_dict[dict_key]

        qty = abs(int(trade_info.qty))
        if trade_info.qty > 0:  # 买
            if trade_info.type == 'OPEN':  # 开仓
                position_db.td_buy_long += qty
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.open_ratio_by_money +
                                  icr_db.open_ratio_by_volume) * qty
            elif trade_info.type == 'CLOSE':  # 平仓
                a = min(qty, position_db.td_sell_short)
                position_db.yd_short_remain -= max(qty - position_db.td_sell_short, 0)
                position_db.td_sell_short -= a
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.close_today_ratio_by_money +
                                  icr_db.close_today_ratio_by_volume) * qty
            elif trade_info.type == 'CLOSE_YESTERDAY':  # 平昨
                position_db.yd_short_remain -= qty
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.close_ratio_by_money +
                                  icr_db.close_ratio_by_volume) * qty

            position_db.day_long += qty
            position_db.day_long_cost = float(position_db.day_long_cost) + \
                                        qty * float(trade_info.price) * float(instrument_all_db.fut_val_pt)
        elif trade_info.qty < 0:  # 卖
            if trade_info.type == 'OPEN':  # 开仓
                position_db.td_sell_short += qty
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.open_ratio_by_money +
                                  icr_db.open_ratio_by_volume) * qty
            elif trade_info.type == 'CLOSE':  # 平仓
                a = min(qty, position_db.td_buy_long)
                position_db.yd_long_remain -= max(qty - position_db.td_buy_long, 0)
                position_db.td_buy_long -= a
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.close_today_ratio_by_money +
                                  icr_db.close_today_ratio_by_volume) * qty
            elif trade_info.type == 'CLOSE_YESTERDAY':  # 平昨
                position_db.yd_long_remain -= qty
                trade_fee = float(trade_info.price * instrument_all_db.fut_val_pt * icr_db.close_ratio_by_money +
                                  icr_db.close_ratio_by_volume) * qty

            position_db.day_short += abs(qty)
            position_db.day_short_cost = float(position_db.day_short_cost) + \
                                         abs(qty) * float(trade_info.price) * float(instrument_all_db.fut_val_pt)
        position_db.fee += Decimal(abs(trade_fee))

    for (symbol, position_db) in position_dict.items():
        session_portfolio.merge(position_db)


def __calculation_position_lts(account_db, trade_list):
    trade_dict = dict()
    for trade_db in trade_list:
        if trade_db.symbol in trade_dict:
            trade_dict[trade_db.symbol].append(trade_db)
        else:
            trade_dict[trade_db.symbol] = [trade_db]

    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id == account_db.accountid, AccountPosition.date == filter_date_str):
        if position_db.symbol not in trade_dict:
            continue
        if position_db.symbol not in instrument_all_dict:
            continue

        trade_list = trade_dict[position_db.symbol]
        trade_list = sorted(trade_list, cmp=lambda x, y: cmp(x.time, y.time))

        instrument_all_db = instrument_all_dict[position_db.symbol]
        if instrument_all_db.type == 'StructuredFund':
            l1r1 = position_db.yd_position_long
            l0r1 = 0
            l1r0 = 0
            for trade_db in trade_list:
                qty = abs(trade_db.qty)
                direction = trade_db.direction
                if direction == 'N':
                    if instrument_all_db.tranche in ('A', 'B'):
                        l1r0 += qty
                    else:
                        a = min(l0r1, qty)
                        l0r1 -= a
                        l1r1 -= max(qty - a, 0)
                elif direction == '0':
                    l0r1 += qty
                elif direction == 'O':
                    if instrument_all_db.tranche in ('A', 'B'):
                        a = min(l0r1, qty)
                        l0r1 -= a
                        l1r1 -= max(qty - a, 0)
                    else:
                        l1r0 += qty
                elif direction == '1':
                    a = min(l1r0, qty)
                    l1r0 -= a
                    l1r1 -= max(qty - a, 0)
            purchase_avail = l1r1 + l0r1
            long_avail = l1r1 + l1r0
            position_db.long_avail = long_avail
            position_db.purchase_avail = purchase_avail
        else:
            creation_redemption_unit = 1
            if (instrument_all_db.pcf is not None) and ('CreationRedemptionUnit' in instrument_all_db.pcf):
                pcf_dict = json.loads(instrument_all_db.pcf)
                creation_redemption_unit = int(float(pcf_dict['CreationRedemptionUnit']))

            l1r1 = position_db.yd_position_long
            l0r1 = 0
            l1r0 = 0
            for trade_db in trade_list:
                qty = abs(trade_db.qty)
                direction = trade_db.direction
                if direction == '2':
                    qty *= creation_redemption_unit
                    l1r0 += qty
                elif direction == '0':
                    l0r1 += qty
                elif direction == '3':
                    qty *= creation_redemption_unit
                    a = min(l0r1, qty)
                    l0r1 -= a
                    l1r1 -= max(qty - a, 0)
                elif direction == '1':
                    a = min(l1r0, qty)
                    l1r0 -= a
                    l1r1 -= max(qty - a, 0)
            purchase_avail = l1r1 + l0r1
            long_avail = l1r1 + l1r0
            if (position_db.yd_position_long > 0) and (long_avail >= 0):
                position_db.long_avail = long_avail
                position_db.purchase_avail = purchase_avail
            else:
                position_db.short_avail = position_db.yd_position_short - purchase_avail
                position_db.purchase_avail = purchase_avail

        position_db.fee = 0
        position_db.day_long = 0
        position_db.day_long_cost = 0
        position_db.day_short = 0
        position_db.day_short_cost = 0
        for trade_db in trade_list:
            if trade_db.qty > 0:  # 买
                if instrument_all_db.type == 'CommonStock':
                    trade_fee = float(trade_db.price) * trade_db.qty * float(instrument_all_db.buy_commission)
                else:
                    trade_fee = 0
                position_db.fee += Decimal(trade_fee)
                position_db.day_long += trade_db.qty
                position_db.day_long_cost = float(position_db.day_long_cost) + \
                                            trade_db.qty * float(trade_db.price) * float(instrument_all_db.fut_val_pt)
            elif trade_db.qty < 0:  # 卖
                if instrument_all_db.type == 'CommonStock':
                    trade_fee = float(trade_db.price) * trade_db.qty * float(instrument_all_db.sell_commission)
                else:
                    trade_fee = 0
                position_db.fee += Decimal(abs(trade_fee))
                position_db.day_short += abs(trade_db.qty)
                position_db.day_short_cost = float(position_db.day_short_cost) + \
                                   abs(trade_db.qty) * float(trade_db.price) * float(instrument_all_db.fut_val_pt)
        session_portfolio.merge(position_db)


def __get_trade_list(account_db):
    trade_list = []
    (start_date, end_date) = date_utils.get_start_end_date()

    query = session_om.query(TradeBroker)
    for trade_db in query.filter(TradeBroker.account == account_db.accountid,
                                 TradeBroker.time >= start_date, TradeBroker.time <= end_date).order_by(TradeBroker.trade_id):
        trade_list.append(trade_db)
    return trade_list


def __rebuild_trade_type_ctp(trade_list):
    for trade_db in trade_list:
        instrument_all_db = instrument_all_dict[trade_db.symbol]
        if instrument_all_db.type in ('Future', 'Option'):
            if trade_db.offsetflag == '0':
                trade_db.type = 'OPEN'  # OPEN
            elif trade_db.offsetflag in ('1', '2', '3'):
                trade_db.type = 'CLOSE'  # CLOSE
            elif trade_db.offsetflag == '4':
                trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
        else:
            trade_db.type = 'NORMAL'  # NORMAL
        session_om.merge(trade_db)


def __rebuild_trade_type_proxy(trade_list):
    for trade_db in trade_list:
        instrument_all_db = instrument_all_dict[trade_db.symbol]
        if instrument_all_db.type in ('Future', 'Option'):
            if trade_db.offsetflag == '48':
                trade_db.type = 'OPEN'  # OPEN
            elif trade_db.offsetflag in ('49', '50', '51'):
                trade_db.type = 'CLOSE'  # CLOSE
            elif trade_db.offsetflag == '52':
                trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
        else:
            trade_db.type = 'NORMAL'  # NORMAL
        session_om.merge(trade_db)


def __rebuild_trade_type_femas(trade_list):
    for trade_db in trade_list:
        instrument_all_db = instrument_all_dict[trade_db.symbol]
        if instrument_all_db.type in ('Future', 'Option'):
            if trade_db.offsetflag == '0':
                trade_db.type = 'OPEN'  # OPEN
            elif trade_db.offsetflag in ('1', '2', '3'):
                trade_db.type = 'CLOSE'  # CLOSE
            elif trade_db.offsetflag == '4':
                trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
        else:
            trade_db.type = 'NORMAL'  # NORMAL
        session_om.merge(trade_db)


def __rebuild_trade_type_lts(trade_list):
    for trade_db in trade_list:
        if trade_db.symbol not in instrument_all_dict:
            continue

        instrument_all_db = instrument_all_dict[trade_db.symbol]
        if trade_db.direction == '0':  # 买
            if instrument_all_db.type in ('Future', 'Option'):
                if trade_db.offsetflag == '0':
                    trade_db.type = 'OPEN'  # OPEN
                elif trade_db.offsetflag == '1' or trade_db.offsetflag == '2' or trade_db.offsetflag == '3':
                    trade_db.type = 'CLOSE'  # CLOSE
                elif trade_db.offsetflag == '4':
                    trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
            else:
                trade_db.type = 'NORMAL'
        elif trade_db.direction == '1':  # 卖
            trade_db.qty = -trade_db.qty
            if instrument_all_db.type in ('Future', 'Option'):
                if trade_db.offsetflag == '0':
                    trade_db.type = 'OPEN'  # OPEN
                elif trade_db.offsetflag == '1' or trade_db.offsetflag == '2' or trade_db.offsetflag == '3':
                    trade_db.type = 'CLOSE'  # CLOSE
                elif trade_db.offsetflag == '4':
                    trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
            else:
                trade_db.type = 'NORMAL'
        elif trade_db.direction == '2':  # ETF申购
            trade_db.type = 'RedPur'
        elif trade_db.direction == '3':  # ETF赎回
            trade_db.qty = -trade_db.qty
            trade_db.type = 'RedPur'
        elif trade_db.direction == 'N':  # SF拆分
            if instrument_all_db.tranche is None or instrument_all_db.tranche == '':  # 母基金
                trade_db.qty = -trade_db.qty
                trade_db.type = 'MergeSplit'
            elif instrument_all_db.tranche == 'A' or instrument_all_db.tranche == 'B':  # 子基金
                trade_db.type = 'MergeSplit'
        elif trade_db.direction == 'O':  # SF合并
            if instrument_all_db.tranche is None or instrument_all_db.tranche == '':  # 母基金
                trade_db.type = 'MergeSplit'
            elif instrument_all_db.tranche == 'A' or instrument_all_db.tranche == 'B':  # 子基金
                trade_db.qty = -trade_db.qty
                trade_db.type = 'MergeSplit'
        session_om.merge(trade_db)


def __rebuild_trade_type_ts(trade_list):
    for trade_db in trade_list:
        if trade_db.symbol not in instrument_all_dict:
            continue

        instrument_all_db = instrument_all_dict[trade_db.symbol]
        if trade_db.direction == '0':  # 买
            if instrument_all_db.type in ('Future', 'Option'):
                if trade_db.offsetflag == '0':
                    trade_db.type = 'OPEN'  # OPEN
                elif trade_db.offsetflag in ('1', '2', '3'):
                    trade_db.type = 'CLOSE'  # CLOSE
                elif trade_db.offsetflag == '4':
                    trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
            else:
                trade_db.type = 'NORMAL'
        elif trade_db.direction == '1':  # 卖
            trade_db.qty = -trade_db.qty
            if instrument_all_db.type in ('Future', 'Option'):
                if trade_db.offsetflag == '0':
                    trade_db.type = 'OPEN'  # OPEN
                elif trade_db.offsetflag in ('1', '2', '3'):
                    trade_db.type = 'CLOSE'  # CLOSE
                elif trade_db.offsetflag == '4':
                    trade_db.type = 'CLOSE_YESTERDAY'  # CLOSE_YESTERDAY
            else:
                trade_db.type = 'NORMAL'
        elif trade_db.direction == '2':  # ETF申购
            trade_db.type = 'RedPur'
        elif trade_db.direction == '3':  # ETF赎回
            trade_db.qty = -trade_db.qty
            trade_db.type = 'RedPur'
        elif trade_db.direction == 'N':  # SF拆分
            if instrument_all_db.tranche is None or instrument_all_db.tranche == '':  # 母基金
                trade_db.qty = -trade_db.qty
                trade_db.type = 'MergeSplit'
            elif instrument_all_db.tranche == 'A' or instrument_all_db.tranche == 'B':  # 子基金
                trade_db.type = 'MergeSplit'
        elif trade_db.direction == 'O':  # SF合并
            if instrument_all_db.tranche is None or instrument_all_db.tranche == '':  # 母基金
                trade_db.type = 'MergeSplit'
            elif instrument_all_db.tranche == 'A' or instrument_all_db.tranche == 'B':  # 子基金
                trade_db.qty = -trade_db.qty
                trade_db.type = 'MergeSplit'
        session_om.merge(trade_db)


def __rebuild_trade_type():
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        trade_list = __get_trade_list(account_db)
        if account_db.accounttype == 'CTP':
            __rebuild_trade_type_ctp(trade_list)
            __calculation_position_ctp(account_db, trade_list)
        elif account_db.accounttype == 'HUABAO':
            __rebuild_trade_type_lts(trade_list)
            __calculation_position_lts(account_db, trade_list)
        elif account_db.accounttype == 'FEMAS':
            __rebuild_trade_type_femas(trade_list)
            __calculation_position_femas(account_db, trade_list)
        elif account_db.accounttype == 'PROXY':
            __rebuild_trade_type_proxy(trade_list)
            __calculation_position_ctp(account_db, trade_list)
        elif account_db.accounttype == 'TS':
            __rebuild_trade_type_ts(trade_list)
            __calculation_position_lts(account_db, trade_list)


if __name__ == '__main__':
    print 'Enter rebuild_trade_calculation_position.'
    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    session_portfolio = server_host.get_db_session('portfolio')
    session_om = server_host.get_db_session('om')

    __build_instrument_dict()
    __rebuild_trade_type()
    session_om.commit()
    session_portfolio.commit()
    server_host.close()
    print 'Exit rebuild_trade_calculation_position.'
