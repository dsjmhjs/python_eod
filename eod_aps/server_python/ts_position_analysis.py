# -*- coding: utf-8 -*-
from decimal import Decimal
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition
from eod_aps.model.schema_common import Instrument
from eod_aps.model.schema_om import OrderBroker, TradeBroker
from eod_aps.model.BaseModel import *
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *

filter_date_str = date_utils.get_today_str('%Y-%m-%d')
now_datetime_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', filter_date_str)
instrument_dict = dict()


def read_position_file_ts(ts_file_path):
    print 'Start read file:' + ts_file_path
    fr = open(ts_file_path)
    account_dict = dict()
    position_dict = dict()
    order_dict = dict()

    for line in fr.readlines():
        base_model = BaseModel()
        for tempStr in line.split(','):
            temp_array = tempStr.replace('\n', '').split(':', 1)
            setattr(base_model, temp_array[0].strip(), temp_array[1].strip())
        msg_type = getattr(base_model, 'MSG_TYPE', '')
        account_name = getattr(base_model, 'ACCOUNT_ID', '')

        if msg_type == 'ACCOUNT':
            account_dict[account_name] = base_model
        elif msg_type == 'ORDER':
            if account_name in order_dict:
                order_dict[account_name].append(base_model)
            else:
                order_dict[account_name] = [base_model]
        elif msg_type == 'POSITION':
            if account_name in position_dict:
                position_dict[account_name].append(base_model)
            else:
                position_dict[account_name] = [base_model]

    for account_name in account_dict.keys():
        account_info_db = __get_account_info(account_name)
        account_id = account_info_db.accountid
        print 'AccountID:', account_id

        __del_order_trader_by_id(account_id)

        order_list = []
        trade_list = []
        if account_name in order_dict:
            order_list, trade_list = __build_order_trade(account_id, order_dict[account_name])

        # position_info_list = []
        # if len(trade_list) > 0:
        #     position_info_list = __get_position_info_list(account_id)
        #     position_db_list = __calculation_position(account_id, position_info_list, trade_list)
        # else:
        #     __del_account_position_by_id(account_id)
        #     if account_name in position_dict:
        #         position_list = position_dict[account_name]
        #     else:
        #         position_list = []
        #     position_db_list = __build_account_position(account_id, position_list)
        position_info_list = []

        __del_account_position_by_id(account_id)
        if account_name in position_dict:
            position_list = position_dict[account_name]
        else:
            position_list = []
        position_db_list = __build_account_position(account_id, position_list)

        cny_position_db = __get_account_cny(account_id, account_dict[account_name], position_info_list)
        position_db_list.append(cny_position_db)

        update_db(order_list, trade_list, position_db_list)
        update_account_trade_restrictions(account_id)


def __build_ticker_exchange(server_model):
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query:
        instrument_dict[instrument_db.ticker.upper()] = instrument_db


def __calculation_position(account_id, position_info_list, trade_list):
    position_info_dict = dict()
    # 根据yd值还原早上持仓
    for position_db in position_info_list:
        position_db.td_buy_long = 0
        position_db.long_avail = position_db.yd_position_long
        position_db.yd_long_remain = position_db.yd_position_long

        position_db.td_sell_short = 0
        position_db.short_avail = position_db.yd_position_short
        position_db.yd_short_remain = position_db.yd_position_short
        position_info_dict[position_db.symbol] = position_db

    for trade_db in trade_list:
        if trade_db.symbol.upper() not in instrument_dict:
            print 'error trade ticker:%s' % trade_db.symbol
            continue
        instrument_db = instrument_dict[trade_db.symbol.upper()]

        if trade_db.symbol not in position_info_dict:
            position_db = AccountPosition()
            position_db.date = filter_date_str
            position_db.id = account_id
            position_db.symbol = trade_db.symbol
            position_db.hedgeflag = 0

            qty = trade_db.qty
            total_cost = trade_db.qty * trade_db.price * instrument_db.fut_val_pt
            if qty >= 0:
                position_db.long = qty
                position_db.long_cost = total_cost
                position_db.long_avail = qty
                position_db.yd_position_long = 0
                position_db.yd_long_remain = 0

                position_db.short = 0
                position_db.short_cost = 0
                position_db.short_avail = 0
                position_db.yd_position_short = 0
                position_db.yd_short_remain = 0

                position_db.day_long = trade_db.qty
                position_db.day_long_cost = trade_db.qty * trade_db.price * instrument_db.fut_val_pt
            else:
                position_db.short = qty
                position_db.short_cost = total_cost
                position_db.short_avail = qty
                position_db.yd_position_short = 0
                position_db.yd_short_remain = 0

                position_db.long = 0
                position_db.long_cost = 0
                position_db.long_avail = 0
                position_db.yd_position_long = 0
                position_db.yd_long_remain = 0

                position_db.day_short = trade_db.qty
                position_db.day_short_cost = trade_db.qty * trade_db.price * instrument_db.fut_val_pt

            position_db.prev_net = 0
            position_db.frozen = 0
            position_db.update_date = date_utils.get_now()
            position_info_dict[position_db.symbol] = position_db
        else:
            position_db = position_info_dict[trade_db.symbol]
            # 期货
            if instrument_db.type_id == 1:
                if trade_db.direction == 0:
                    # 买开
                    if trade_db.offsetflag == 0:
                        position_db.td_buy_long += trade_db.qty
                        position_db.long_cost += trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                    # 买平
                    elif trade_db.offsetflag == 1:
                        a = min(trade_db.qty, position_db.short)
                        position_db.yd_short_remain -= max(trade_db.qty - position_db.short, 0)
                        position_db.td_sell_short -= a
                        position_db.short_cost -= trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                    # 买平昨
                    elif trade_db.offsetflag == 4:
                        position_db.yd_short_remain -= trade_db.qty

                    position_db.day_long += trade_db.qty
                    position_db.day_long_cost += trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                elif trade_db.direction == 1:
                    # 卖开
                    if trade_db.offsetflag == 0:
                        position_db.td_sell_short += trade_db.qty
                        position_db.short_cost += trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                    # 卖平
                    elif trade_db.offsetflag == 1:
                        a = min(trade_db.qty, position_db.long)
                        position_db.yd_long_remain -= max(trade_db.qty - position_db.long, 0)
                        position_db.td_buy_long -= a
                        position_db.long_cost -= trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                    # 卖平昨
                    elif trade_db.offsetflag == 4:
                        position_db.yd_long_remain -= trade_db.qty

                    position_db.day_short += trade_db.qty
                    position_db.day_short_cost += trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                position_db.long_avail = position_db.td_buy_long + position_db.yd_long_remain
                position_db.short_avail = position_db.td_sell_short + position_db.yd_short_remain

                position_db.long = position_db.long_avail
                position_db.short = position_db.short_avail
            # 股票
            elif instrument_db.type_id == 4:
                if trade_db.direction == 0:
                    position_db.long += trade_db.qty
                    position_db.long_cost += trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                elif trade_db.direction == 1:
                    position_db.long -= trade_db.qty
                    position_db.long_cost -= trade_db.qty * trade_db.price * instrument_db.fut_val_pt
                    position_db.short_avail -= trade_db.qty
            position_info_dict[position_db.symbol] = position_db

    result_list = []
    for key in position_info_dict.keys():
        result_list.append(position_info_dict[key])
    return result_list


def __get_account_info(account_name):
    print account_name
    account_info_db = session_portfolio.query(RealAccount).filter(RealAccount.accountname == account_name).first()
    return account_info_db


def __get_position_info_list(account_id):
    position_info_list = []
    query = session_portfolio.query(AccountPosition)
    for pf_position_db in query.filter(AccountPosition.id == account_id, AccountPosition.date == filter_date_str):
        position_info_list.append(pf_position_db)
    return position_info_list


def __del_account_position_by_id(account_id):
    del_sql = "delete from portfolio.account_position where ID= '%s' and DATE ='%s'" % (account_id, filter_date_str)
    session_portfolio.execute(del_sql)
    session_portfolio.commit()


def __del_order_trader_by_id(account_id):
    del_sql = "delete from om.order_broker where ACCOUNT=%s and INSERT_TIME LIKE '%s'" % \
              (account_id, filter_date_str + '%')
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME LIKE '%s'" % \
              (account_id, filter_date_str + '%')
    session_om.execute(del_sql)


def __build_order_trade(account_id, order_message_list):
    order_list = []
    trade_list = []
    order_dict = dict()
    for order_info in order_message_list:
        order_db = OrderBroker()

        order_db.sys_id = getattr(order_info, 'ORDER_ID', '')
        # sys_id为空表示交易未到达交易所即被打回
        # if order_db.sys_id == '':
        #     continue

        order_db.account = account_id

        message_symbol = getattr(order_info, 'SYMBOL', 'NULL').split('.')[0]
        if message_symbol not in instrument_dict:
            continue
        instrumet_db = instrument_dict[message_symbol]
        order_db.symbol = instrumet_db.ticker

        # 成交量
        filled_quantity = getattr(order_info, 'FILLED_QUANTITY', '')
        if filled_quantity >= 0:
            # 0:买，1：卖
            order_db.direction = 0
        else:
            order_db.direction = 1

        action = getattr(order_info, 'ACTION', '')
        if action == 'BUYTOOPEN':
            # 0:开仓  1:平仓  3:平今  4:平昨
            order_db.trade_type = 0
        elif action == 'BUYTOCLOSE':
            if 'yd' in order_db.sys_id:
                order_db.trade_type = 4
            else:
                order_db.trade_type = 1
        elif action == 'SELLTOOPEN':
            order_db.trade_type = 0
        elif action == 'SELLTOCLOSE':
            if 'yd' in order_db.sys_id:
                order_db.trade_type = 4
            else:
                order_db.trade_type = 1
        elif action == 'BUY':
            order_db.direction = 0
            order_db.trade_type = 2
        elif action == 'SELL':
            order_db.direction = 1
            order_db.trade_type = 2

        status = getattr(order_info, 'STATUS', '')
        if status == 'RECEIVED':
            # 全部成交:'0' 部分成交还在队列中:'1',部分成交不在队列中:'2',未成交还在队列中:'3',
            # 未成交不在队列中:'4',撤单:'5',未知:'a',尚未触发:'b',已触发:'c'
            continue
        elif status == 'CANCELED':
            order_db.status = 5
        elif status == 'FILLED':
            order_db.status = 0
        elif status == 'PARTIALLYFILLED':
            order_db.status = 1

        # 已经提交:'0',撤单已经提交:'1',修改已经提交:'2',已经接受:'3',
        # 报单已经被拒绝:'4',撤单已经被拒绝:'5',改单已经被拒绝:'6'
        # order_db.submit_status = getattr(order_info, 'OrderSubmitStatus', '')
        insert_time_str = getattr(order_info, 'ENTERED_TIME', '')
        order_db.insert_time = insert_time_str.replace('/', '-')

        order_db.qty = getattr(order_info, 'ENTERED_QUANTITY', 0)

        order_db.price = getattr(order_info, 'LIMIT_PRICE', 0)

        # order_db.ex_qty = getattr(order_info, 'VolumeTraded', '')
        order_list.append(order_db)
        if status == 'FILLED':
            trade_db = __build_trade(account_id, order_info)
            trade_list.append(trade_db)

        order_dict[order_db.sys_id] = order_db

    order_list = sorted(order_list, cmp=lambda x, y: cmp(x.insert_time, y.insert_time))
    trade_list = sorted(trade_list, cmp=lambda x, y: cmp(x.time, y.time))
    return order_list, trade_list


def __build_trade(account_id, order_info):
    trade_db = TradeBroker()
    trade_db.account = account_id
    trade_db.order_id = getattr(order_info, 'ORDER_ID', '')

    trade_db.trade_id = getattr(order_info, 'SYS_ORDER_ID', '')

    trade_db.time = getattr(order_info, 'FILL_TIME', '').replace('/', '-')

    message_symbol = getattr(order_info, 'SYMBOL', 'NULL').split('.')[0]
    instrumet_db = instrument_dict[message_symbol]
    trade_db.symbol = instrumet_db.ticker

    # 成交量
    filled_quantity = getattr(order_info, 'FILLED_QUANTITY', '')
    trade_db.qty = Decimal(filled_quantity)

    trade_db.price = Decimal(getattr(order_info, 'FILLED_PRICE', ''))

    # 普通成交:'0'|期权执行:'1'|OTC成交:'2'|期转现衍生成交:'3'|组合衍生成交:'4'
    trade_db.trade_type = 0
    # 开仓:'0'|平仓:'1'|强平:'2'|平今:'3'|平昨:'4'|强减:'5'|本地强平:'6'
    action = getattr(order_info, 'ACTION', '')
    if action == 'BUYTOOPEN':
        # 0:开仓  1:平仓  3:平今  4:平昨
        trade_db.offsetflag = 0
        # 0:买，1：卖
        trade_db.direction = 0
    elif action == 'BUYTOCLOSE':
        if 'yd' in trade_db.order_id:
            trade_db.offsetflag = 4
        else:
            trade_db.offsetflag = 1
        trade_db.direction = 0
    elif action == 'SELLTOOPEN':
        trade_db.offsetflag = 0
        trade_db.direction = 1
    elif action == 'SELLTOCLOSE':
        if 'yd' in trade_db.order_id:
            trade_db.offsetflag = 4
        else:
            trade_db.offsetflag = 1
        trade_db.direction = 1
    elif action == 'BUY':
        trade_db.trade_type = 0
        trade_db.direction = 0
    elif action == 'SELL':
        trade_db.trade_type = 0
        trade_db.direction = 1
    trade_db.hedgeflag = 0
    return trade_db


def update_db(order_list, trade_list, position_db_list):
    for order_db in order_list:
        session_om.merge(order_db)
    for trade_db in trade_list:
        session_om.merge(trade_db)
    for position_db in position_db_list:
        session_portfolio.merge(position_db)


def __get_account_cny(account_id, base_model, pre_position_info_list):
    account_cny_db = None
    for position_info in pre_position_info_list:
        if position_info.symbol == 'CNY':
            account_cny_db = position_info

    if account_cny_db is None:
        account_cny_db = AccountPosition()
    account_cny_db.date = filter_date_str
    account_cny_db.id = account_id
    account_cny_db.symbol = 'CNY'

    # 获取实时账户净值的数值
    account_cny_db.long = getattr(base_model, 'RT_ACCOUNT_NET_WORTH', '0')
    # 获取实时现金余额的数值
    account_cny_db.long_avail = getattr(base_model, 'RT_CASH_BALANCE', '0')
    # 获取账户日初始净值的数值
    account_cny_db.prev_net = getattr(base_model, 'BD_ACCOUNT_NET_WORTH', '0')
    account_cny_db.update_date = date_utils.get_now()
    return account_cny_db


def __build_account_position(account_id, investor_position_array):
    position_list = []
    for investor_position in investor_position_array:
        position_db = AccountPosition()
        position_db.date = filter_date_str
        position_db.id = account_id
        message_symbol = getattr(investor_position, 'SYMBOL', 'NULL').split('.')[0]
        if message_symbol not in instrument_dict:
            continue
        instrumet_db = instrument_dict[message_symbol]
        position_db.symbol = instrumet_db.ticker

        position_db.hedgeflag = 0

        long_qty_message = int(getattr(investor_position, 'LONG_QUANTITY', 0))
        long_avail_qty_message = int(getattr(investor_position, 'AVAILABLE_QUANTITY', 0))
        short_qty_message = abs(int(getattr(investor_position, 'SHORT_QUANTITY', 0)))
        if long_qty_message < 0:
            long_qty = 0
            long_avail_qty = 0
            short_qty = abs(long_qty_message)
        else:
            long_qty = long_qty_message
            long_avail_qty = long_avail_qty_message
            short_qty = short_qty_message

        average_price = float(getattr(investor_position, 'AVERAGE_PRICE', 0))

        position_db.long = long_qty
        position_db.long_cost = long_qty * average_price * float(instrumet_db.fut_val_pt)
        position_db.long_avail = long_avail_qty
        position_db.yd_position_long = long_qty
        position_db.yd_long_remain = long_qty

        position_db.short = short_qty
        position_db.short_cost = short_qty * average_price * float(instrumet_db.fut_val_pt)
        position_db.short_avail = short_qty
        position_db.yd_position_short = short_qty
        position_db.yd_short_remain = short_qty

        position_db.prev_net = position_db.yd_position_long - position_db.yd_position_short
        position_db.frozen = 0
        position_db.update_date = date_utils.get_now()
        position_list.append(position_db)
    return position_list


def update_account_trade_restrictions(account_id):
    (start_date, end_date) = date_utils.get_start_end_date()
    cancle_order_dict = dict()
    order_dict = dict()
    query = session_om.query(OrderBroker)
    for order_db in query.filter(OrderBroker.account == account_id,
                                 OrderBroker.insert_time >= start_date,
                                 OrderBroker.insert_time <= end_date):
        if order_db.status == 5:
            symbol = order_db.symbol
            if 'IC' in symbol:
                symbol = 'SH000905'
            elif 'IF' in symbol:
                symbol = 'SHSZ300'
            elif 'IH' in symbol:
                symbol = 'SSE50'
            if symbol in cancle_order_dict:
                cancle_order_dict[symbol].append(order_db)
            else:
                cancle_order_dict[symbol] = [order_db]
        order_dict[order_db.sys_id] = order_db

    for (symbol, order_db_list) in cancle_order_dict.items():
        update_sql = "update portfolio.account_trade_restrictions set TODAY_CANCEL = %s where \
                TICKER = '%s' and ACCOUNT_ID = %s" % (len(order_db_list), symbol, account_id)
        session_portfolio.execute(update_sql)

    open_trade_dict = dict()
    query = session_om.query(TradeBroker)
    for trade_db in query.filter(TradeBroker.account == account_id,
                                 TradeBroker.time >= start_date,
                                 TradeBroker.time <= end_date):
        order_id = trade_db.order_id
        if order_id not in order_dict:
            print 'Error order_id:', order_id
            continue
        order_db = order_dict[order_id]
        # 开仓
        if order_db.trade_type != 0:
            continue

        symbol = trade_db.symbol
        if 'IC' in symbol:
            symbol = 'SH000905'
        elif 'IF' in symbol:
            symbol = 'SHSZ300'
        elif 'IH' in symbol:
            symbol = 'SSE50'

        if symbol in open_trade_dict:
            open_trade_dict[symbol] += abs(int(trade_db.qty))
        else:
            open_trade_dict[symbol] = abs(int(trade_db.qty))
    for (symbol, today_open) in open_trade_dict.items():
        update_sql = "update portfolio.account_trade_restrictions set TODAY_OPEN = %s where \
                TICKER = '%s' and ACCOUNT_ID = %s" % (today_open, symbol, account_id)
        session_portfolio.execute(update_sql)


def ts_position_analysis():
    print 'Enter ts_position_analysis.'
    server_host = server_constant_local.get_server_model('host')
    global session_common
    session_common = server_host.get_db_session('common')
    global session_portfolio
    session_portfolio = server_host.get_db_session('portfolio')
    global session_om
    session_om = server_host.get_db_session('om')

    __build_ticker_exchange(server_host)
    ts_position_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('ysposition', filter_date_str)
    for ts_file in ts_position_file_list:
        read_position_file_ts('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, ts_file))

    session_common.commit()
    session_portfolio.commit()
    session_om.commit()
    server_host.close()
    print 'Exit ts_position_analysis.'


if __name__ == '__main__':
    ts_position_analysis()
