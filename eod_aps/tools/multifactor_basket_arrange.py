# -*- coding: utf-8 -*-
# 整理股票市值和对冲市值相差较大的篮子
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.schema_common import Instrument
from eod_aps.model.eod_const import const
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from decimal import Decimal
from eod_aps.tools.stock_wind_utils import StockWindUtils

date_utils = DateUtils()


def __query_instrument_dict(session_common):
    global instrument_dict
    instrument_dict = dict()
    query = session_common.query(Instrument)
    for instrument_db in query:
        instrument_dict[instrument_db.ticker] = instrument_db


def __query_pf_position(session_portfolio, pf_account_db, filter_date_str):
    pf_position_dict = dict()
    query = session_portfolio.query(PfPosition)
    for pf_position_db in query.filter(PfPosition.id == pf_account_db.id, PfPosition.date == filter_date_str):
        pf_position_dict[pf_position_db.symbol] = pf_position_db
    return pf_position_dict


def __sum_pf_position(pf_position_dict, filter_date_str):
    with StockWindUtils() as stock_wind_utils:
        ticker_type_list = [const.INSTRUMENT_TYPE_ENUMS.CommonStock, const.INSTRUMENT_TYPE_ENUMS.Future]
        common_ticker_list = stock_wind_utils.get_ticker_list(ticker_type_list)
        ticker_price_dict = stock_wind_utils.get_close_dict(filter_date_str, common_ticker_list)

    stock_total_value = Decimal(0.0)
    future_total_value = Decimal(0.0)
    for (symbol, pf_position_db) in pf_position_dict.items():
        if symbol not in ticker_price_dict:
            print 'Error Ticker:', symbol
            continue

        instrument_db = instrument_dict[symbol]
        prev_close = ticker_price_dict[symbol]
        temp_value = (pf_position_db.long - pf_position_db.short) * Decimal(prev_close) * instrument_db.fut_val_pt
        if symbol.isdigit():
            stock_total_value += temp_value
        else:
            future_total_value += temp_value
    print '%.f,%.f' % (stock_total_value, future_total_value)
    return stock_total_value, abs(future_total_value)


def multifactor_basket_arrange(server_name, pf_account_id):
    filter_date_str = date_utils.get_next_trading_day()

    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    pf_position_arrange_list = []
    query_pf_account = session_portfolio.query(PfAccount)
    pf_account_db = query_pf_account.filter(PfAccount.id == pf_account_id).first()
    print pf_account_db.fund_name

    fund_name = pf_account_db.fund_name.split('-')[2]
    default_fund_name = 'default-manual-%s-' % fund_name
    default_pf_account_db = query_pf_account.filter(PfAccount.fund_name == default_fund_name).first()

    # 計算當前籃子差值
    pf_position_dict = __query_pf_position(session_portfolio, pf_account_db, filter_date_str)
    stock_total_value, future_total_value = __sum_pf_position(pf_position_dict, filter_date_str)
    if stock_total_value == 0:
        return
    diff_weight = (stock_total_value - future_total_value) / stock_total_value

    # 計算調倉
    arrange_info_list = []
    for (symbol, pf_position_db) in pf_position_dict.items():
        if not symbol.isdigit():
            continue

        change_volume = __rounding_number(pf_position_db.long * Decimal(diff_weight))
        pf_position_db.long -= change_volume
        pf_position_db.long_avail -= change_volume
        # arrange_info_list.append('%s,%s,%s' % (symbol, sell_volume, pf_position_db.long))
        arrange_info_list.append('%s,%s' % (symbol, change_volume))
        pf_position_arrange_list.append(pf_position_db)
    __sum_pf_position(pf_position_dict, filter_date_str)

    # 調倉移到default
    default_pf_position_dict = __query_pf_position(session_portfolio, default_pf_account_db, filter_date_str)
    for arrange_info in arrange_info_list:
        symbol, change_volume = arrange_info.split(',')
        change_volume = int(change_volume)
        if symbol in default_pf_position_dict:
            default_position_db = default_pf_position_dict[symbol]
            target_volume = default_position_db.long - default_position_db.short + change_volume
            if target_volume > 0:
                default_position_db.long = target_volume
                default_position_db.long_avail = target_volume
                default_position_db.short = 0
                default_position_db.short_avail = 0
            else:
                default_position_db.long = 0
                default_position_db.long_avail = 0
                default_position_db.short = abs(target_volume)
                default_position_db.short_avail = abs(target_volume)
        else:
            default_position_db = PfPosition()
            default_position_db.date = pf_position_db.date
            default_position_db.id = default_pf_account_db.id
            default_position_db.symbol = symbol
            default_position_db.hedgeflag = 0
            if change_volume > 0:
                default_position_db.long = change_volume
                default_position_db.long_avail = change_volume
                default_position_db.short = 0
                default_position_db.short_avail = 0
            else:
                default_position_db.long = 0
                default_position_db.long_avail = 0
                default_position_db.short = abs(change_volume)
                default_position_db.short_avail = abs(change_volume)
            default_position_db.long_cost = 0
            default_position_db.yd_position_long = 0
            default_position_db.yd_long_remain = 0
            default_position_db.short_cost = 0
            default_position_db.yd_position_short = 0
            default_position_db.yd_short_remain = 0
            default_position_db.prev_net = 0
        pf_position_arrange_list.append(default_position_db)

    for position_db in pf_position_arrange_list:
        session_portfolio.merge(position_db)
    session_portfolio.commit()
    session_portfolio.close()


def __rounding_number(number_input):
    # 对数字进行四舍五入
    return int(round(float(number_input) / float(100), 0) * 100)


if __name__ == '__main__':
    server_model = server_constant.get_server_model('host')
    session_common = server_model.get_db_session('common')
    __query_instrument_dict(session_common)
    for pf_account_id in ('499',):
        multifactor_basket_arrange('guoxin', pf_account_id)
    # for pf_account_id in ('407','408','409','425','426','427','428','429','430','431','432','433','434','470','471','472','473','474','496','497','498','499','500','501','502','503','504','505'):
    #     multifactor_basket_arrange('guoxin', pf_account_id)
    # for pf_account_id in ('409','425','426','427','428'):
    #     multifactor_basket_arrange('host', pf_account_id)


