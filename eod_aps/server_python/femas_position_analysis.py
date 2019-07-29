# -*- coding: utf-8 -*-
from datetime import datetime
from eod_aps.model.BaseModel import *
from eod_aps.model.schema_om import OrderBroker, TradeBroker
from eod_aps.model.schema_portfolio import AccountPosition
from eod_aps.tools.file_utils import FileUtils
from eod_aps.model.eod_const import const
from eod_aps.server_python import *

now = datetime.now()
today_str = now.strftime('%Y-%m-%d')

order_list = []
trade_list = []
position_list = []

orderDict = dict()
tradeDict = dict()


def read_position_file_femas(femas_file_path):
    print 'Start read file:' + femas_file_path
    fr = open(femas_file_path)
    order_array = []
    trade_array = []
    trading_account_array = []
    investor_position_array = []
    for line in fr.readlines():
        if 'Account_ID' in line:
            account_id = line.replace('\n', '').split(':')[1]
        else:
            base_model = BaseModel()
            for temp_str in line.split('|')[1].split(','):
                temp_array = temp_str.replace('\n', '').split(':', 1)
                setattr(base_model, temp_array[0].strip(), temp_array[1])
            if 'OnRspQryOrder' in line:
                order_array.append(base_model)
            elif 'OnRspQryTrade' in line:
                trade_array.append(base_model)
            elif 'OnRspQryInvestorAccount' in line:
                trading_account_array.append(base_model)
            elif 'OnRspQryInvestorPosition' in line:
                investor_position_array.append(base_model)

    print 'AccountID:', account_id
    # 删除该账号今日记录
    del_account_position_by_id(account_id)
    del_order_trader_by_id(account_id)

    save_order(account_id, order_array)
    save_trade(account_id, trade_array)
    save_account_cny(account_id, trading_account_array)
    save_account_position(account_id, investor_position_array)

    update_db()
    update_position_prev_net(account_id)


def del_account_position_by_id(account_id):
    del_sql = "delete from portfolio.account_position where ID= '%s' and DATE ='%s'" % (account_id, today_str)
    session_portfolio.execute(del_sql)


def del_order_trader_by_id(account_id):
    del_sql = "delete from om.order_broker where ACCOUNT=%s and INSERT_TIME LIKE '%s'" % (account_id, today_str + '%')
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME LIKE '%s'" % (account_id, today_str + '%')
    session_om.execute(del_sql)


def save_order(account_id, order_array):
    for order_info in order_array:
        order_db = OrderBroker()
        order_db.sys_id = getattr(order_info, 'OrderSysID', '')
        order_db.account = account_id
        order_db.symbol = getattr(order_info, 'InstrumentID', '')
        order_db.direction = getattr(order_info, 'Direction', '')
        order_db.trade_type = getattr(order_info, 'OffsetFlag', '')
        order_db.status = getattr(order_info, 'OrderStatus', '')

        order_submit_status = getattr(order_info, 'OrderSubmitStatus', '')
        if order_submit_status == '':
            order_submit_status = order_db.status
        order_db.submit_status = order_submit_status

        trading_day = getattr(order_info, 'TradingDay', '')
        insert_time = getattr(order_info, 'InsertTime', '')
        if (trading_day != '') and (insert_time != ''):
            insert_time_str = '%s-%s-%s %s' % (trading_day[0:4], trading_day[4:6], trading_day[6:8], insert_time)
        else:
            insert_time_str = '%s 00:00:00' % (today_str,)
        order_db.insert_time = insert_time_str

        order_db.qty = getattr(order_info, 'Volume', '')
        order_db.price = getattr(order_info, 'LimitPrice', '')
        order_db.ex_qty = getattr(order_info, 'VolumeTraded', '')
        order_list.append(order_db)


def save_trade(account_id, trade_array):
    for trade_info in trade_array:
        trade_db = TradeBroker()
        trade_db.trade_id = getattr(trade_info, 'TradeID', '')

        trading_day = getattr(trade_info, 'TradingDay', '')
        insert_time = getattr(trade_info, 'InsertTime', '')
        if (trading_day != '') and (insert_time != ''):
            insert_time_str = '%s-%s-%s %s' % (trading_day[0:4], trading_day[4:6], trading_day[6:8], insert_time)
        else:
            insert_time_str = '%s 00:00:00' % (today_str,)
        trade_db.time = insert_time_str

        trade_db.symbol = getattr(trade_info, 'InstrumentID', '')
        trade_db.qty = getattr(trade_info, 'TradeVolume', '')
        trade_db.price = getattr(trade_info, 'TradePrice', '')
        trade_db.account = account_id
        trade_db.order_id = getattr(trade_info, 'OrderSysID', '')
        # 买:'0'|卖:'1'
        trade_db.direction = getattr(trade_info, 'Direction', '')
        # 开仓:'0'|平仓:'1'|强平:'2'|平今:'3'|平昨:'4'
        trade_db.offsetflag = getattr(trade_info, 'OffsetFlag', '')

        hedge_flag = getattr(trade_info, 'HedgeFlag', '1')
        if hedge_flag == '':
            trade_db.hedgeflag = '0'
        else:
            trade_db.hedgeflag = const.HEDGE_FLAG_MAP[hedge_flag]
        trade_list.append(trade_db)


def save_account_cny(account_id, message_array):
    for trading_account in message_array:
        position_db = AccountPosition()
        position_db.date = today_str
        position_db.id = account_id
        position_db.symbol = 'CNY'

        available = getattr(trading_account, 'Available', '0')
        margin = getattr(trading_account, 'Margin', '0')
        position_db.long = float(available) + float(margin)
        position_db.long_avail = available
        position_db.prev_net = getattr(trading_account, 'PreBalance', '0')
        position_db.update_date = datetime.now()
        position_list.append(position_db)


def save_account_position(account_id, investor_position_array):
    tick_position_dict = dict()
    for investor_position in investor_position_array:
        symbol = getattr(investor_position, 'InstrumentID', 'NULL')
        hedge_flag = getattr(investor_position, 'HedgeFlag', '0')
        if hedge_flag == '1':
            hedge_flag = '0'
        elif hedge_flag == '2':
            hedge_flag = '1'
        elif hedge_flag == '3':
            hedge_flag = '2'
        key = '%s|%s' % (symbol, hedge_flag)
        if key in tick_position_dict:
            tick_position_dict.get(key).append(investor_position)
        else:
            tick_position_dict[key] = [investor_position]

    for (key, tickPositionList) in tick_position_dict.items():
        (symbol, hedge_flag) = key.split('|')

        td_long = 0
        td_long_cost = 0.0
        td_long_avail = 0

        yd_long = 0
        yd_long_remain = 0

        td_short = 0
        td_short_cost = 0.0
        td_short_avail = 0

        yd_short = 0
        yd_short_remain = 0

        long_frozen = 0
        short_frozen = 0
        prev_net = 0

        for temp_position in tickPositionList:
            # 转换hedgeFlag字典
            direction = getattr(temp_position, 'Direction', 'NULL')
            position = int(getattr(temp_position, 'Position', 'NULL'))
            position_cost = getattr(temp_position, 'PositionCost', 'NULL')
            yd_position = int(getattr(temp_position, 'YdPosition', 'NULL'))
            if direction == '0':
                td_long = position
                td_long_avail = position
                td_long_cost = position_cost
                yd_long = yd_position
            else:
                td_short = position
                td_short_avail = position
                td_short_cost = position_cost
                yd_short = yd_position

        prev_net = yd_long - yd_short
        (yd_long_remain, yd_short_remain) = calculation_by_trade(symbol, yd_long, yd_short)

        position_db = AccountPosition()
        position_db.date = today_str
        position_db.id = account_id
        position_db.symbol = symbol
        position_db.hedgeflag = hedge_flag
        position_db.long = td_long
        position_db.long_cost = td_long_cost
        position_db.long_avail = td_long_avail
        position_db.short = td_short
        position_db.short_cost = td_short_cost
        position_db.short_avail = td_short_avail
        position_db.yd_position_long = yd_long
        position_db.yd_position_short = yd_short
        position_db.yd_long_remain = yd_long_remain
        position_db.yd_short_remain = yd_short_remain
        position_db.prev_net = prev_net
        # position_db.purchase_avail = purchase_avail
        position_db.frozen = long_frozen
        position_db.update_date = datetime.now()
        position_list.append(position_db)


def calculation_by_trade(symbol, yd_long, yd_short):
    td_long_avail = 0
    yd_long_remain = yd_long

    td_short_avail = 0
    yd_short_remain = yd_short
    if symbol in tradeDict:
        for tradeInfo in tradeDict[symbol]:
            offset_flag = getattr(tradeInfo, 'OffsetFlag', '')
            qty = int(getattr(tradeInfo, 'TradeVolume', ''))
            trade_direction = getattr(tradeInfo, 'Direction', '')
            if trade_direction == '0':
                if offset_flag == '0':
                    td_long_avail += qty
                elif offset_flag == '1':
                    a = min(td_short_avail, qty)
                    td_short_avail -= a
                    yd_short_remain -= max(qty - a, 0)
                elif offset_flag == '3':
                    td_short_avail -= qty
                elif offset_flag == '4':
                    yd_short_remain -= qty
            else:
                if offset_flag == '0':
                    td_short_avail += qty
                elif offset_flag == '1':
                    a = min(td_long_avail, qty)
                    td_long_avail -= a
                    yd_long_remain -= max(qty - a, 0)
                elif offset_flag == '3':
                    td_long_avail -= qty
                elif offset_flag == '4':
                    yd_long_remain -= qty
    td_long = td_long_avail + yd_long_remain
    td_short = td_short_avail + yd_short_remain
    return yd_long_remain, yd_short_remain


def update_db():
    for order_db in order_list:
        session_om.add(order_db)
    for trade_db in trade_list:
        session_om.add(trade_db)
    for position_db in position_list:
        session_portfolio.add(position_db)


def update_position_prev_net(account_id):
    update_sql = "Update portfolio.account_position Set PREV_NET = YD_POSITION_LONG - YD_POSITION_SHORT where ID=%s \
and DATE='%s' and symbol != 'CNY'" % (account_id, today_str)
    session_portfolio.execute(update_sql)


if __name__ == '__main__':
    print 'Enter femas_position_analysis.'
    server_host = server_constant_local.get_server_model('host')
    session_portfolio = server_host.get_db_session('portfolio')
    session_om = server_host.get_db_session('om')

    femas_position_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('FEMAS_POSITION', today_str)
    for femas_file in femas_position_file_list:
        read_position_file_femas('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, femas_file))

    session_portfolio.commit()
    session_om.commit()
    server_host.close()
    print 'Exit femas_position_analysis.'
