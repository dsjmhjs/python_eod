# -*- coding: utf-8 -*-
# 恒生交易系统持仓数据更新
from datetime import datetime
from eod_aps.model.schema_om import OrderBroker, TradeBroker
from eod_aps.model.schema_portfolio import AccountPosition
from eod_aps.model.BaseModel import *
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *

now = datetime.now()
filter_date_str = date_utils.get_today_str('%Y-%m-%d')
now_datetime_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', filter_date_str)

CITICS_HEDGE_FLAG_MAP = {'49': '0', }
CITICS_DIRECTION_MAP = {'48': '2', '49': '3'}


def read_position_file_hs(hs_file_path, add_flag):
    print 'Start read file:' + hs_file_path
    fr = open(hs_file_path)
    order_array = []
    trade_array = []
    trading_account_array = []
    investor_position_array = []
    for line in fr.readlines():
        if line.strip() == '':
            continue

        if 'Account_ID' in line:
            account_id = line.replace('\n', '').split(':')[1]
        else:
            base_model = BaseModel()
            for tempStr in line.split('|')[1].split(','):
                temp_array = tempStr.replace('\n', '').split(':', 1)
                if len(temp_array) != 2:
                    continue
                setattr(base_model, temp_array[0].strip(), temp_array[1].strip())
            if 'QryOrder' in line:
                order_array.append(base_model)
            elif 'QryTrade' in line:
                trade_array.append(base_model)
            elif 'QryCash' in line:
                trading_account_array.append(base_model)
            elif 'QryPosition' in line:
                investor_position_array.append(base_model)

    print 'AccountID:', account_id

    # 删除该账号今日记录
    del_account_position_by_id(account_id)

    (order_list, order_dict) = __get_order_list(account_id, order_array)
    trade_list = __get__trade_list(account_id, trade_array, order_dict)

    (position_dict, position_db_list) = __build_account_position(account_id, investor_position_array)

    cny_position_db = __get_account_cny(account_id, trading_account_array)
    position_db_list.append(cny_position_db)
    update_db(order_list, trade_list, position_db_list)


def __calculation_position(trade_list, position_dict):
    trade_list = sorted(trade_list, cmp=lambda x, y: cmp(x.time, y.time))
    for trade_info in trade_list:
        dict_key = '%s|%s' % (trade_info.symbol, trade_info.hedge_flag)
        if dict_key not in position_dict:
            print 'error trade:', trade_info.print_info()
            continue
        position_db = position_dict[dict_key]
        qty = int(trade_info.qty)
        if trade_info.direction == '0':  # 买:0
            if trade_info.offsetflag == '0':  # 开仓
                position_db.td_buy_long += qty
            elif trade_info.offsetflag == '1':  # 平仓
                a = min(qty, position_db.td_sell_short)
                position_db.yd_short_remain -= max(qty - position_db.td_sell_short, 0)
                position_db.td_sell_short -= a
            elif trade_info.offsetflag == '3':  # 平今
                position_db.td_sell_short -= qty
            elif trade_info.offsetflag == '4':  # 平昨
                position_db.yd_short_remain -= qty
        elif trade_info.direction == '1':
            if trade_info.offsetflag == '0':  # 开仓
                position_db.td_sell_short += qty
            elif trade_info.offsetflag == '1':  # 平仓
                a = min(qty, position_db.td_buy_long)
                position_db.yd_long_remain -= max(qty - position_db.td_buy_long, 0)
                position_db.td_buy_long -= a
            elif trade_info.offsetflag == '3':  # 平今
                position_db.td_buy_long -= qty
            elif trade_info.offsetflag == '4':  # 平昨
                position_db.yd_long_remain -= qty

    position_db_list = []
    for (symbol, position_db) in position_dict.items():
        position_db_list.append(position_db)
    return position_db_list


def update_db(order_list, trade_list, position_db_list):
    for order_db in order_list:
        session_om.merge(order_db)
    for trade_db in trade_list:
        session_om.merge(trade_db)
    for position_db in position_db_list:
        session_portfolio.merge(position_db)


def del_account_position_by_id(account_id):
    del_sql = "delete from portfolio.account_position where ID= '%s' and DATE ='%s'" % (account_id, filter_date_str)
    session_portfolio.execute(del_sql)

    del_sql = "delete from om.order_broker where ACCOUNT=%s and INSERT_TIME LIKE '%s'" % (
    account_id, filter_date_str + '%')
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME LIKE '%s'" % (account_id, filter_date_str + '%')
    session_om.execute(del_sql)


def del_order_trader_by_id(account_id):
    (start_date, end_date) = date_utils.get_start_end_date()
    del_sql = "delete from om.order_broker where ACCOUNT=%s and INSERT_TIME>'%s'" % (account_id, start_date)
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME>'%s'" % (account_id, start_date)
    session_om.execute(del_sql)


def __get_order_list(account_id, order_array):
    order_list = []
    order_dict = dict()
    for order_info in order_array:
        order_db = OrderBroker()
        order_db.sys_id = getattr(order_info, 'report_no', '')
        # sys_id为空表示交易未到达交易所即被打回
        if order_db.sys_id == '':
            continue

        order_db.account = account_id
        order_db.symbol = getattr(order_info, 'stock_code', '')

        # 48:买，49：卖
        order_db.direction = getattr(order_info, 'entrust_bs', '')

        # # 0:开仓  1:平仓  3:平今  4:平昨
        # order_db.trade_type = getattr(order_info, 'm_eOffsetFlag', '')
        order_db.trade_type = 0

        # 全部成交:'0' 部分成交还在队列中:'1',部分成交不在队列中:'2',未成交还在队列中:'3',
        # 未成交不在队列中:'4',撤单:'5',未知:'a',尚未触发:'b',已触发:'c'
        order_db.status = getattr(order_info, 'entrust_status', '')

        # # 已经提交:'0',撤单已经提交:'1',修改已经提交:'2',已经接受:'3',报单已经被拒绝:'4',撤单已经被拒绝:'5',改单已经被拒绝:'6'
        # order_db.submit_status = getattr(order_info, 'EEntrustSubmitStatus', '')
        order_db.submit_status = 0

        trading_day = getattr(order_info, 'entrust_date', '')
        insert_time = getattr(order_info, 'entrust_time', '').zfill(6)
        if (trading_day != '') and (insert_time != ''):
            insert_time_str = '%s-%s-%s %s:%s:%s' % (trading_day[0:4], trading_day[4:6], trading_day[6:8],
                                                     insert_time[:2], insert_time[2:4], insert_time[4:6])
        else:
            insert_time_str = '%s 00:00:00' % (filter_date_str,)
        order_db.insert_time = insert_time_str

        qty = getattr(order_info, 'entrust_amount', '')
        if order_db.direction == '1':
            order_db.qty = qty
        elif order_db.direction == '2':
            order_db.qty = 0 - float(qty)
        order_db.price = getattr(order_info, 'entrust_price', '')
        order_db.ex_qty = getattr(order_info, 'business_amount', '')
        order_list.append(order_db)
        order_dict[order_db.sys_id] = order_db
    return order_list, order_dict


def __get__trade_list(account_id, trade_array, order_dict):
    trade_list = []
    for trade_info in trade_array:
        trade_db = TradeBroker()
        trade_db.symbol = getattr(trade_info, 'stock_code', '')
        trade_db.order_id = getattr(trade_info, 'report_no', '')
        if trade_db.order_id not in order_dict:
            print '[Error]unfind OrderID:', trade_db.order_id
            continue

        trade_db.trade_id = getattr(trade_info, 'business_no', '')

        trading_day = getattr(trade_info, 'date', '')
        insert_time = getattr(trade_info, 'business_time', '').zfill(6)
        if (trading_day != '') and (insert_time != ''):
            insert_time_str = '%s-%s-%s %s:%s:%s' % (trading_day[0:4], trading_day[4:6], trading_day[6:8],
                                                     insert_time[:2], insert_time[2:4], insert_time[4:6])
            if insert_time > now_datetime_str:
                insert_time_str = '%s %s' % (last_trading_day, insert_time)
        else:
            insert_time_str = '%s 00:00:00' % (filter_date_str,)
        trade_db.time = insert_time_str

        order_db = order_dict[trade_db.order_id]
        qty = getattr(trade_info, 'business_amount', '')
        if order_db.direction == '1':
            trade_db.qty = qty
        elif order_db.direction == '2':
            trade_db.qty = 0 - float(qty)

        trade_db.price = getattr(trade_info, 'business_price', '')

        # 普通成交:'0'|期权执行:'1'|OTC成交:'2'|期转现衍生成交:'3'|组合衍生成交:'4'
        trade_db.trade_type = 0

        # trade_db.offsetflag = getattr(trade_info, 'm_nOffsetFlag', '')
        trade_db.offsetflag = 0

        trade_db.account = account_id
        trade_db.direction = getattr(trade_info, 'entrust_bs', '')

        # hedge_flag = getattr(trade_info, 'm_nHedgeFlag', '')
        # trade_db.hedgeflag = HEDGE_FLAG_MAP[hedge_flag]
        trade_db.hedgeflag = 0

        trade_list.append(trade_db)
    return trade_list


def __get_account_cny(account_id, message_array):
    position_db = AccountPosition()
    for trading_account in message_array:
        money_type = getattr(trading_account, 'money_type', '0')
        if money_type != '0':
            continue

        position_db.date = filter_date_str
        position_db.id = account_id
        position_db.symbol = 'CNY'

        position_db.long = getattr(trading_account, 'fund_balance', '0')
        position_db.long_avail = getattr(trading_account, 'enable_balance', '0')
        position_db.prev_net = getattr(trading_account, 'fund_balance', '0')
        position_db.update_date = datetime.now()
    return position_db


def __build_account_position(account_id, investor_position_array):
    position_list = []
    position_dict = dict()
    ticker_position_dict = dict()
    for investorPosition in investor_position_array:
        symbol = getattr(investorPosition, 'stock_code', 'NULL')
        # 过滤掉SP j1609&j1701这种的持仓数据
        if '&' in symbol:
            continue

        # 转换hedgeFlag字典
        # hedge_flag = getattr(investorPosition, 'm_nHedgeFlag', '0')
        # hedge_flag = HEDGE_FLAG_MAP[hedge_flag]
        hedge_flag = 0

        key = '%s|%s' % (symbol, hedge_flag)
        if key in ticker_position_dict:
            ticker_position_dict.get(key).append(investorPosition)
        else:
            ticker_position_dict[key] = [investorPosition]

    for (key, ticker_position_list) in ticker_position_dict.items():
        (symbol, hedge_flag) = key.split('|')
        td_long = 0
        td_long_cost = 0.0
        td_long_avail = 0

        for temp_position in ticker_position_list:
            td_long += float(getattr(temp_position, 'current_amount', '0'))
            td_long_cost += float(getattr(temp_position, 'cost_price', '0'))
            td_long_avail += float(getattr(temp_position, 'enable_amount', '0'))

        position_db = AccountPosition()
        position_db.date = filter_date_str
        position_db.id = account_id
        position_db.symbol = symbol
        position_db.hedgeflag = hedge_flag
        position_db.long = td_long
        position_db.long_cost = td_long_cost
        position_db.long_avail = td_long_avail
        position_db.short = 0
        position_db.short_cost = 0
        position_db.short_avail = 0

        position_db.ys_position_long = td_long
        position_db.yd_long_remain = td_long
        position_db.ys_position_short = 0
        position_db.yd_short_remain = 0
        position_db.prev_net = td_long
        position_db.frozen = 0
        position_db.update_date = datetime.now()
        position_dict[key] = position_db
        position_list.append(position_db)
    return position_dict, position_list


def __account_position_enter(add_flag):
    print 'Enter hs_position_analysis add_account_position.'
    server_host = server_constant_local.get_server_model('host')
    session_portfolio = server_host.get_db_session('portfolio')
    session_om = server_host.get_db_session('om')
    global session_portfolio, session_om

    hs_position_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('PROXYCITICS_POSITION',
                                                                                  filter_date_str)
    for hs_file in hs_position_file_list:
        read_position_file_hs('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, hs_file), add_flag)

    session_portfolio.commit()
    session_om.commit()
    server_host.close()
    print 'Exit hs_position_analysis add_account_position.'


def add_account_position():
    __account_position_enter(True)


if __name__ == '__main__':
    add_account_position()
