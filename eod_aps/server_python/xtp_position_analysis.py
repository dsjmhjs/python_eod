# -*- coding: utf-8 -*-
# 中泰持仓数据更新
from eod_aps.model.schema_om import OrderBroker, TradeBroker
from eod_aps.model.schema_portfolio import AccountPosition
from eod_aps.model.BaseModel import *
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *

filter_date_str = date_utils.get_today_str('%Y-%m-%d')
now_datetime_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', filter_date_str)


def read_position_file(xtp_file_path, add_flag):
    print 'Start read file:' + xtp_file_path

    order_array = []
    trade_array = []
    trading_account_array = []
    investor_position_array = []
    with open(xtp_file_path) as fr:
        for line in fr.readlines():
            if 'Account_ID' in line:
                account_id = line.replace('\n', '').split(':')[1]
            else:
                base_model = BaseModel()
                for tempStr in line.split('|')[1].split(','):
                    temp_array = tempStr.replace('\n', '').split(':', 1)
                    setattr(base_model, temp_array[0].strip(), temp_array[1].strip())
                if 'OnQryOrder' in line:
                    order_array.append(base_model)
                elif 'OnQryTrade' in line:
                    trade_array.append(base_model)
                elif 'OnQryAsset' in line:
                    trading_account_array.append(base_model)
                elif 'OnQryPosition' in line:
                    investor_position_array.append(base_model)

    print 'AccountID:', account_id

    if add_flag:
        # 删除该账号今日记录
        del_account_position_by_id(account_id)

    order_list = __get_order_list(account_id, order_array)
    trade_list = __get__trade_list(account_id, trade_array)

    (position_dict, position_db_list) = __build_account_position(account_id, investor_position_array)

    cny_position_db = __get_account_cny(account_id, trading_account_array)
    position_db_list.append(cny_position_db)
    update_db(order_list, trade_list, position_db_list)


def __get_order_list(account_id, order_array):
    order_list = []
    for order_info in order_array:
        order_db = OrderBroker()
        order_db.sys_id = getattr(order_info, 'order_xtp_id', '')
        order_db.account = account_id
        order_db.symbol = getattr(order_info, 'ticker', '')
        # 1:买，2：卖
        order_db.direction = getattr(order_info, 'side', '')
        # 0:开仓  1:平仓  3:平今  4:平昨
        order_db.trade_type = 0
        #
        order_db.status = getattr(order_info, 'order_status', '')
        # # 已经提交:'0',撤单已经提交:'1',修改已经提交:'2',已经接受:'3',报单已经被拒绝:'4',撤单已经被拒绝:'5',改单已经被拒绝:'6'
        order_db.submit_status = getattr(order_info, 'order_submit_status', '')

        insert_time = getattr(order_info, 'insert_time', '')
        if insert_time != '':
            insert_time_str = '%s-%s-%s %s:%s:%s' % (insert_time[0:4], insert_time[4:6], insert_time[6:8],
                                                     insert_time[:2], insert_time[2:4], insert_time[4:6])
        else:
            insert_time_str = '%s 00:00:00' % (filter_date_str,)
        order_db.insert_time = insert_time_str

        order_db.price = getattr(order_info, 'price', '')
        qty = getattr(order_info, 'quantity', '')
        if order_db.direction == '1':
            order_db.qty = qty
        elif order_db.direction == '2':
            order_db.qty = 0 - float(qty)
        order_db.ex_qty = getattr(order_info, 'qty_traded', '')
        order_list.append(order_db)
    return order_list


def __get__trade_list(account_id, trade_array):
    trade_list = []
    for trade_info in trade_array:
        trade_db = TradeBroker()
        trade_db.account = account_id
        trade_db.hedgeflag = 0
        trade_db.symbol = getattr(trade_info, 'ticker', '')
        trade_db.order_id = getattr(trade_info, 'order_xtp_id', '')
        trade_db.trade_id = getattr(trade_info, 'exec_id', '')

        trade_time = getattr(trade_info, 'trade_time', '')
        if trade_time != '':
            insert_time_str = '%s-%s-%s %s:%s:%s' % (trade_time[0:4], trade_time[4:6], trade_time[6:8],
                                                     trade_time[:2], trade_time[2:4], trade_time[4:6])
        else:
            insert_time_str = '%s 00:00:00' % (filter_date_str,)
        trade_db.time = insert_time_str

        trade_db.direction = getattr(trade_info, 'side', '')
        qty = getattr(trade_info, 'quantity', '')
        if trade_db.direction == '1':
            trade_db.qty = qty
        elif trade_db.direction == '2':
            trade_db.qty = 0 - float(qty)

        trade_db.price = getattr(trade_info, 'price', '')
        # 普通成交:'0'|期权执行:'1'|OTC成交:'2'|期转现衍生成交:'3'|组合衍生成交:'4'
        trade_db.trade_type = 0
        trade_db.offsetflag = 0
        trade_list.append(trade_db)
    return trade_list


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

    del_sql = "delete from om.order_broker where ACCOUNT=%s and INSERT_TIME LIKE '%s'" % (account_id, filter_date_str + '%')
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME LIKE '%s'" % (account_id, filter_date_str + '%')
    session_om.execute(del_sql)


def __get_account_cny(account_id, message_array):
    position_db = AccountPosition()
    for trading_account in message_array:
        position_db.date = filter_date_str
        position_db.id = account_id
        position_db.symbol = 'CNY'

        position_db.long = getattr(trading_account, 'total_asset', '0')
        position_db.long_avail = getattr(trading_account, 'buying_power', '0')
        position_db.prev_net = position_db.long_avail
        position_db.update_date = date_utils.get_now()
    return position_db


def __build_account_position(account_id, investor_position_array):
    last_position_dict = dict()
    query_position = session_portfolio.query(AccountPosition)
    for position_db in query_position.filter(AccountPosition.id == account_id, AccountPosition.date == filter_date_str):
        if position_db.symbol == 'CNY':
            continue
        last_position_dict[position_db.symbol] = position_db

    position_list = []
    position_dict = dict()
    for investorPosition in investor_position_array:
        symbol = getattr(investorPosition, 'ticker', 'NULL')
        # 过滤掉SP j1609&j1701这种的持仓数据
        if '&' in symbol:
            continue

        hedge_flag = 0
        td_long = getattr(investorPosition, 'total_qty', '0')
        avg_price = getattr(investorPosition, 'avg_price', '0')
        td_long_cost = float(td_long) * float(avg_price)
        td_long_avail = getattr(investorPosition, 'sellable_qty', '0')

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

        position_db.yd_long_remain = td_long
        position_db.yd_short_remain = 0
        position_db.prev_net = td_long
        position_db.frozen = 0
        position_db.update_date = date_utils.get_now()
        position_list.append(position_db)
    return position_dict, position_list


def __account_position_enter(add_flag):
    print 'Enter xtp_position_analysis.'
    global session_portfolio, session_om
    server_host = server_constant_local.get_server_model('host')
    session_portfolio = server_host.get_db_session('portfolio')
    session_om = server_host.get_db_session('om')

    xtp_position_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('PROXYXTP_POSITION', filter_date_str)
    for xtp_file in xtp_position_file_list:
        read_position_file('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, xtp_file), add_flag)

    session_portfolio.commit()
    session_om.commit()
    server_host.close()
    print 'Exit xtp_position_analysis.'


def add_account_position():
    __account_position_enter(True)


def update_account_position():
    __account_position_enter(False)


if __name__ == '__main__':
    add_account_position()

