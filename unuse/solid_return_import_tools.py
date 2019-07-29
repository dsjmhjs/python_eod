# -*- coding: utf-8 -*-
import os
import sys
from itertools import islice
from decimal import Decimal
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.trade2_history import Trade2History
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.stock_utils import StockUtils

reload(sys)
sys.setdefaultencoding('utf8')

date_utils = DateUtils()
stock_utils = StockUtils()
server_constant = ServerConstant()
#  base_file_path = 'E:/dailyFiles/ts_test'
base_file_path = 'Z:/temp/wangjian/Trade Log'

strategy_name_dict = {u'增持': 'Shareholder', u'内部人买入': 'Insider', u'员工持股计划': 'Esop',
                      u'股权激励': 'Incentive', u'大宗交易': 'Block', u'评级上调': 'Analyst', u'机构调研': 'IR',
                      u'业绩预增': 'Profit', u'主动': 'Active', u'对冲': 'Hedge'}

server_name = 'host'

save_path_base = 'Z:/temp/luolinhua/general_display/Event_Real/LongOnly'
# r = redis.Redis(host='172.16.12.118', port=6379, db=1)


def __read_position_file(date_str, file_name):
    pf_position_dict = dict()
    input_file = open('%s/%s' % (base_file_path, file_name))
    for line in islice(input_file, 1, None):
        line_item = line.strip('\n').split(',')
        ticker = line_item[0].split('.')[0]
        strategy_name = line_item[2].decode('gb2312')
        if strategy_name not in strategy_name_dict:
            print strategy_name
            continue
        strategy_name = strategy_name_dict[strategy_name]
        pf_account_db = pf_account_dict[strategy_name]
        volume = Decimal(line_item[4])

        pf_position_db.date = date_str
        pf_position_db.id = pf_account_db.id
        pf_position_db = PfPosition()
        pf_position_db.symbol = ticker
        pf_position_db.hedgeflag = 0
        instrument_db = instrument_dict[ticker]

        prev_close = stock_utils.get_prev_close(date_str.replace('-', ''), ticker)
        if prev_close is None:
            print 'unfind prev_close,date:%s,ticker:%s' % (date_str, ticker)
            prev_close = 0
        else:
            prev_close = Decimal(prev_close)

        if volume >= 0:
            pf_position_db.long = volume
            pf_position_db.long_cost = volume * prev_close * instrument_db.fut_val_pt
            pf_position_db.long_avail = volume
            pf_position_db.yd_position_long = volume
            pf_position_db.yd_long_remain = volume
        else:
            volume = abs(volume)
            pf_position_db.short = volume
            pf_position_db.short_cost = volume * prev_close * instrument_db.fut_val_pt
            pf_position_db.short_avail = volume
            pf_position_db.yd_position_short = volume
            pf_position_db.yd_short_remain = volume
        pf_position_db.prev_net = pf_position_db.yd_position_long - pf_position_db.yd_position_short

        dict_key = '%s|%s' % (pf_position_db.id, pf_position_db.symbol)
        if dict_key in pf_position_dict:
            temp_pf_position_db = pf_position_dict[dict_key]
            pf_position_db.merge(temp_pf_position_db)
            pf_position_dict[dict_key] = pf_position_db
        else:
            pf_position_dict[dict_key] = pf_position_db
    print len(list(pf_position_dict.values()))
    return list(pf_position_dict.values())


def __read_trade_file(date_str, file_name):
    trade2_history_list = []
    input_file = open('%s/%s' % (base_file_path, file_name))
    for line in islice(input_file, 1, None):
        line_item = line.strip('\n').split(',')
        ticker = line_item[5]
        if ticker.isdigit():
            ticker = ticker.zfill(6)
        # 多空标记
        ls_flag = line_item[7].decode('gb2312')
        # 买卖标记
        bs_flag = line_item[8].decode('gb2312')
        price = line_item[10]
        volume = Decimal(line_item[11].decode('gb2312').replace('股', '').replace('手', '').replace('张', ''))
        strategy_name = line_item[15].decode('gb2312')
        # sub_strategy_name = line_item[16].decode('gb2312')
        if strategy_name not in strategy_name_dict:
            print strategy_name
            continue
        strategy_name = strategy_name_dict[strategy_name]
        pf_account_db = pf_account_dict[strategy_name]

        trade2_history_db = Trade2History()
        trade2_history_db.time = '%s 15:30:00' % date_str
        trade2_history_db.symbol = ticker
        trade2_history_db.price = price
        if ls_flag == u'买入' and bs_flag == u'卖出':
            trade2_history_db.trade_type = 3
        elif ls_flag == u'卖出' and bs_flag == u'买入':
            trade2_history_db.trade_type = 2
        else:
            trade2_history_db.trade_type = 0
        trade2_history_db.strategy_id = pf_account_db.fund_name

        if ls_flag == u'买入':
            trade2_history_db.qty = volume
        elif ls_flag == u'卖出':
            trade2_history_db.qty = -volume
        elif bs_flag == u'买入':
            trade2_history_db.qty = volume
        elif bs_flag == u'卖出':
            trade2_history_db.qty = -volume

        trade2_history_db.account = ''
        trade2_history_list.append(trade2_history_db)
    print len(trade2_history_list)
    return trade2_history_list


def __build_pf_account_dict():
    global pf_account_dict
    pf_account_dict = dict()
    server_host = ServerConstant().get_server_model(server_name)
    session_portfolio = server_host.get_db_session('portfolio')
    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.group_name == 'Event_Real'):
        pf_account_dict[pf_account_db.name] = pf_account_db


def __save_to_db(trade2_history_list, pf_position_list):
    server_model = ServerConstant().get_server_model(server_name)
    session_om = server_model.get_db_session('om')
    for trade2_history_db in trade2_history_list:
        session_om.add(trade2_history_db)
    session_om.commit()

    session_portfolio = server_model.get_db_session('portfolio')
    for pf_position_db in pf_position_list:
        session_portfolio.add(pf_position_db)
    session_portfolio.commit()


def solid_return_import_tools(date_str):
    global instrument_dict
    instrument_dict = stock_utils.build_instrument_dict()
    __build_pf_account_dict()

    position_file_name = None
    trade_file_name = None
    for file_name in os.listdir(base_file_path):
        file_name = file_name.decode('gb2312')
        if date_str.replace('-', '') not in file_name:
            continue
        if u'持仓' in file_name:
            position_file_name = file_name
        elif u'成交' in file_name:
            trade_file_name = file_name
    print position_file_name, trade_file_name

    trade2_history_list = []
    pf_position_list = []
    if trade_file_name is not None:
        trade2_history_list = __read_trade_file(date_str, trade_file_name)
    if position_file_name is not None:
        pf_position_list = __read_position_file(date_str, position_file_name)
    __save_to_db(trade2_history_list, pf_position_list)


def __build_ticker_exchange(server_model):
    global instrument_db_dict
    instrument_db_dict = dict()
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_((1, 4))):
        instrument_db_dict[instrument_db.ticker] = instrument_db


def dailyreturn_calculation(server_name, group_name):
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    __build_ticker_exchange(server_model)

    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.group_name.like('%' + group_name + '%')):
        __dailyreturn_calculation(server_model, pf_account_db)


def __dailyreturn_calculation(server_model, pf_account_db):
    # redis_title_name = 'return_rate:solidreturn_list'
    # r.lpush(redis_title_name, pf_account_db.fund_name)

    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')

    trade2_history_dict = dict()
    query_trade = session_om.query(Trade2History)
    for trade2_history_db in query_trade.filter(Trade2History.strategy_id == pf_account_db.fund_name):
        date_str = trade2_history_db.time.strftime("%Y-%m-%d")
        if date_str in trade2_history_dict:
            trade2_history_dict[date_str].append(trade2_history_db)
        else:
            trade2_history_dict[date_str] = [trade2_history_db]

    query_pf_position = session_portfolio.query(PfPosition)
    pf_position_dict = dict()
    for pf_position_db in query_pf_position.filter(PfPosition.id == pf_account_db.id):
        date_str = pf_position_db.date.strftime("%Y-%m-%d")
        if date_str in pf_position_dict:
            pf_position_dict[date_str].append(pf_position_db)
        else:
            pf_position_dict[date_str] = [pf_position_db]

    trading_date_list = pf_position_dict.keys()
    trading_date_list.sort()

    future_open_dict = dict()
    money_surplus_pool = 0.0
    prev_equity_total = None
    # redis_key = 'return_rate:solidreturn:%s' % pf_account_db.fund_name
    report_result_list = []
    for date_str in trading_date_list:
        total_buy_money = 0.0
        total_sell_money = 0.0
        total_money_change = 0.0
        if date_str in trade2_history_dict:
            trade2_history_list = trade2_history_dict[date_str]
            for trade_db in trade2_history_list:
                ticker = trade_db.symbol.split(' ')[0]
                if 'IC' in ticker:
                    margin_ratio = 0.3
                elif 'IF' in ticker:
                    margin_ratio = 0.2

                instrument_db = instrument_db_dict[ticker]
                if trade_db.trade_type == 0:
                    if trade_db.qty > 0:
                        total_buy_money += float(trade_db.price) * abs(trade_db.qty) * (1 + 0.00025)
                    else:
                        total_sell_money += float(trade_db.price) * abs(trade_db.qty) * (1 - 0.00125)
                elif trade_db.trade_type == 2:
                    total_buy_money += float(trade_db.price) * abs(trade_db.qty) * float(instrument_db.fut_val_pt) * (
                        margin_ratio + 0.000026)
                    if ticker not in future_open_dict:
                        future_open_dict[ticker] = [trade_db]
                    else:
                        future_open_dict[ticker].append(trade_db)
                elif trade_db.trade_type == 3:
                    future_open_trade = future_open_dict[ticker][0]
                    if trade_db.qty <= abs(future_open_trade.qty):
                        total_sell_money += float(future_open_trade.price) * abs(trade_db.qty) \
* float(instrument_db.fut_val_pt) * margin_ratio + (float(future_open_trade.price) - float(trade_db.price)) * abs(trade_db.qty) \
* float(instrument_db.fut_val_pt) * (1 - 0.000026)
                    else:
                        for future_open_trade in future_open_dict[ticker]:
                            total_sell_money += float(future_open_trade.price) * abs(future_open_trade.qty) \
* float(instrument_db.fut_val_pt) * margin_ratio + (float(future_open_trade.price) - float(trade_db.price)) * abs(future_open_trade.qty) \
* float(instrument_db.fut_val_pt) * (1 - 0.000026)
            total_money_change = total_buy_money - total_sell_money

        # 现金剩余量
        if total_money_change < 0:
            money_surplus_pool += abs(total_money_change)
        elif total_money_change > 0:
            money_surplus_pool -= min(total_money_change, money_surplus_pool)

        stock_equity = 0.0
        future_equity = 0.0
        pf_position_list = pf_position_dict[date_str]
        for pf_position_db in pf_position_list:
            instrument_db = instrument_db_dict[pf_position_db.symbol]
            ticker_close_price = stock_utils.get_close(date_str.replace('-', ''), pf_position_db.symbol)
            if ticker_close_price is None:
                print date_str, pf_position_db.symbol
                continue

            if instrument_db.type_id == 4:
                stock_equity += float(pf_position_db.long) * float(ticker_close_price)
            elif instrument_db.type_id == 1:
                if 'IC' in instrument_db.ticker:
                    margin_ratio = 0.3
                elif 'IF' in instrument_db.ticker:
                    margin_ratio = 0.2

                future_open_trade = future_open_dict[instrument_db.ticker][0]
                if abs(pf_position_db.short) <= abs(future_open_trade.qty):
                    future_equity += float(abs(pf_position_db.short)) * float(future_open_trade.price) * float(
                        instrument_db.fut_val_pt) * margin_ratio \
                                     + (float(future_open_trade.price) - float(ticker_close_price)) * float(
                        abs(pf_position_db.short)) * float(instrument_db.fut_val_pt)
                else:
                    for future_open_trade in future_open_dict[instrument_db.ticker]:
                        future_equity += float(abs(future_open_trade.qty)) * float(future_open_trade.price) * float(
                            instrument_db.fut_val_pt) * margin_ratio \
                                         + (float(future_open_trade.price) - float(ticker_close_price)) * float(
                            abs(future_open_trade.qty)) * float(instrument_db.fut_val_pt)

        equity_total = stock_equity + future_equity
        if prev_equity_total is None:
            pnl = equity_total - total_money_change
            equity_base = total_money_change
        else:
            pnl = equity_total - total_money_change - prev_equity_total
            if total_money_change > 0:
                equity_base = prev_equity_total + money_surplus_pool + total_money_change
            elif total_money_change < 0:
                equity_base = prev_equity_total + money_surplus_pool + total_money_change
            else:
                equity_base = prev_equity_total + money_surplus_pool

        return_rate = pnl / float(equity_base)
        # report_result_list.append('%s,%s,%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f%%' \
        #                      % (date_str, total_buy_money, total_sell_money, total_money_change, money_surplus_pool
        #                 , prev_equity_total, stock_equity, future_equity, equity_total, pnl, equity_base, return_rate))
        report_result_list.append('%s,%.5f' % (date_str, return_rate))
        prev_equity_total = equity_total
        # r.hset(redis_key, date_str, '%.2f' % return_rate)
    report_result_list = list(reversed(sorted(report_result_list)))
    report_result_list.insert(0, 'Date,Strategy')
    file_object = open('%s/%s.csv' % (save_path_base, pf_account_db.fund_name), 'w+')
    file_object.write('\n'.join(report_result_list))
    file_object.close()


def dailyreturn_test(server_name, group_name):
    server_model = ServerConstant().get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')

    __build_ticker_exchange(server_model)

    query_pf_account = session_portfolio.query(PfAccount)
    for pf_account_db in query_pf_account.filter(PfAccount.group_name.like('%' + group_name + '%')):
        __dailyreturn_test(server_model, pf_account_db)


def __dailyreturn_test(server_model, pf_account_db):
    print '-----------', pf_account_db.fund_name
    session_portfolio = server_model.get_db_session('portfolio')
    session_om = server_model.get_db_session('om')

    trade2_history_dict = dict()
    query_trade = session_om.query(Trade2History)
    for trade2_history_db in query_trade.filter(Trade2History.strategy_id == pf_account_db.fund_name):
        date_str = trade2_history_db.time.strftime("%Y-%m-%d")
        ticker = trade2_history_db.symbol.split(' ')[0]
        dict_key = '%s|%s' % (date_str, ticker)
        if dict_key in trade2_history_dict:
            trade2_history_dict[dict_key] += trade2_history_db.qty
        else:
            trade2_history_dict[dict_key] = trade2_history_db.qty

    query_pf_position = session_portfolio.query(PfPosition)
    pf_position_dict = dict()
    for pf_position_db in query_pf_position.filter(PfPosition.id == pf_account_db.id):
        date_str = pf_position_db.date.strftime("%Y-%m-%d")
        dict_key = '%s|%s' % (date_str, pf_position_db.symbol)
        pf_position_dict[dict_key] = pf_position_db.long_avail

    for (dict_key, trade_volume) in trade2_history_dict.items():
        if dict_key in pf_position_dict:
            position_volume = pf_position_dict[dict_key]
        else:
            position_volume = 0

        date_str, ticker = dict_key.split('|')
        prev_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)
        find_dict_key = '%s|%s' % (prev_date_str, ticker)
        if find_dict_key in pf_position_dict:
            prev_position_volume = pf_position_dict[find_dict_key]
        else:
            prev_position_volume = 0

        if prev_position_volume + trade_volume != position_volume:
            print date_str, ticker, prev_position_volume, trade_volume, position_volume




if __name__ == '__main__':
    # today_filter_str = date_utils.get_today_str('%Y-%m-%d')
    #
    # trading_day_list = date_utils.get_trading_day_list(date_utils.string_toDatetime('2017-05-31'),
    #                                                    date_utils.string_toDatetime('2017-06-05'))
    # for trading_day in trading_day_list:
    #     solid_return_import_tools(trading_day.strftime("%Y-%m-%d"))
    # dailyreturn_test('host', 'Event_Real')
    dailyreturn_calculation('host', 'Event_Real')
