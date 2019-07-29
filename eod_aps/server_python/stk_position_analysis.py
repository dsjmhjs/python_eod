# -*- coding: utf-8 -*-
from eod_aps.model.BaseModel import *
from eod_aps.tools.file_utils import FileUtils
from eod_aps.model.schema_om import OrderBroker, TradeBroker
from eod_aps.model.schema_portfolio import AccountPosition
from eod_aps.server_python import *


today_str = date_utils.get_today_str('%Y-%m-%d')
order_list = []
trade_list = []
position_list = []

accountName = ''
accountSuffix = ''

orderDict = dict()
tradeDict = dict()


def read_position_file_stk(stk_file_path):
    print 'Start read file:' + stk_file_path
    fr = open(stk_file_path)
    order_array = []
    trade_array = []
    expendable_fund_array = []
    expendable_shares_array = []
    for line in fr.readlines():
        if 'Account_ID' in line:
            account_id = line.replace('\n', '').split(':')[1]
        else:
            base_model = BaseModel()
            for tempStr in line.split('|')[1].split(','):
                tempArray = tempStr.replace('\n', '').split(':', 1)
                setattr(base_model, tempArray[0].strip(), tempArray[1])
            if 'OnRspQryCurrDayOrder' in line:
                order_array.append(base_model)
            elif 'OnRspQryCurrDayFill' in line:
                trade_array.append(base_model)
            elif 'OnRspQryExpendableFund' in line:
                expendable_fund_array.append(base_model)
            elif 'OnRspQryExpendableShares' in line:
                expendable_shares_array.append(base_model)

    print 'AccountID:', account_id
    # 删除该账号今日记录
    del_account_position_by_id(account_id)
    del_order_trader_by_id(account_id)
    save_order(account_id, order_array)
    save_trade(account_id, trade_array)

    save_account_cny(account_id, expendable_fund_array)
    save_account_position(account_id, expendable_shares_array)

    update_db()


def update_db():
    for order_db in order_list:
        session_om.add(order_db)
    for trade_db in trade_list:
        session_om.add(trade_db)
    for position_db in position_list:
        session_portfolio.add(position_db)


def del_account_position_by_id(account_id):
    del_sql = "delete from portfolio.account_position where ID=%s and DATE='%s'" % (account_id, today_str)
    session_portfolio.execute(del_sql)


def del_order_trader_by_id(account_id):
    del_sql = "delete from om.order_broker where ACCOUNT=%s AND INSERT_TIME  LIKE '%s'" % (account_id, today_str + '%')
    session_om.execute(del_sql)

    del_sql = "delete from om.trade2_broker where ACCOUNT=%s and TIME LIKE '%s'" % (account_id, today_str + '%')
    session_om.execute(del_sql)


def save_order(account_id, order_array):
    for order_info in order_array:
        order_db = OrderBroker()
        order_db.sys_id = getattr(order_info, 'szOrderId', '').strip()
        order_db.account = account_id
        order_db.symbol = getattr(order_info, 'szStkCode', '')

        # 证券业务行为
        order_db.direction = getattr(order_info, 'iStkBizAction', '')

        order_db.trade_type = 0
        order_db.status = getattr(order_info, 'chOrderStatus', '')

        order_db.insert_time = getattr(order_info, 'szOrderTime', '')

        order_db.qty = getattr(order_info, 'llOrderQty', '')
        order_db.price = getattr(order_info, 'szOrderAmt', '')
        order_db.ex_qty = getattr(order_info, 'llMatchedQty', '')
        order_list.append(order_db)


def save_trade(account_id, trade_array):
    for trade_info in trade_array:
        trade_db = TradeBroker()
        trade_db.trade_id = getattr(trade_info, 'szQryPos', '').strip()
        trade_db.hedgeflag = 0

        trading_day = getattr(trade_info, 'iOrderDate', '')
        insert_time = getattr(trade_info, 'szMatchedTime', '')
        if (trading_day != '') and (insert_time != ''):
            insert_time_str = '%s-%s-%s %s:%s:%s' % (trading_day[0:4], trading_day[4:6], trading_day[6:8],
                                                       insert_time[:2], insert_time[2:4], insert_time[4:6])
        else:
            insert_time_str = '%s 00:00:00' % (today_str,)
        trade_db.time = insert_time_str

        trade_db.symbol = getattr(trade_info, 'szStkCode', '')
        trade_db.qty = getattr(trade_info, 'szMatchedQty', '')
        trade_db.price = getattr(trade_info, 'szMatchedPrice', '')
        trade_db.trade_type = getattr(trade_info, 'chMatchedType', '')
        trade_db.account = account_id
        trade_db.order_id = getattr(trade_info, 'szOrderId', '')
        trade_db.direction = getattr(trade_info, 'iStkBizAction', '')
        trade_db.offsetflag = 0
        trade_list.append(trade_db)


def save_account_cny(account_id, message_array):
    for trading_account in message_array:
        position_db = AccountPosition()
        position_db.date = today_str
        position_db.id = account_id
        position_db.symbol = 'CNY'

        position_db.long = getattr(trading_account, 'szFundBln', 'NULL')
        position_db.long_avail = getattr(trading_account, 'szFundAvl', 'NULL')
        position_db.prev_net = getattr(trading_account, 'szFundPrebln', '0')
        position_db.update_date = date_utils.get_now()
        position_list.append(position_db)


def save_account_position(account_id, investorPositionArray):
    for investorPosition in investorPositionArray:
        symbol = getattr(investorPosition, 'szStkCode', 'NULL')
        yd_long = getattr(investorPosition, 'llStkPrebln', '0')
        td_long = getattr(investorPosition, 'llStkBln', '0')
        td_long_avail = getattr(investorPosition, 'llStkAvl', '0')
        td_long_cost = getattr(investorPosition, 'szStkBcostRlt', '0')
        long_frozen = getattr(investorPosition, 'llStkTrdFrz', '0')

        yd_short = 0
        td_short = 0
        td_short_cost = 0
        td_short_avail = 0

        yd_long_remain = yd_long
        yd_short_remain = yd_short
        purchase_avail = yd_long
        prev_net = float(yd_long) - float(yd_short)

        position_db = AccountPosition()
        position_db.date = today_str
        position_db.id = account_id
        position_db.symbol = symbol
        position_db.hedgeflag = 0
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
        position_db.frozen = long_frozen
        position_db.update_date = date_utils.get_now()
        position_list.append(position_db)


def stk_position_analysis():
    print 'Enter Stk_position_analysis.'
    host_server_model = server_constant_local.get_server_model('host')
    global session_om
    session_om = host_server_model.get_db_session('om')
    global session_portfolio
    session_portfolio = host_server_model.get_db_session('portfolio')

    stk_position_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('PROXYKMA_POSITION', today_str)
    for stk_file in stk_position_file_list:
        read_position_file_stk('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, stk_file))

    session_om.commit()
    session_portfolio.commit()
    print 'Exit Stk_position_analysis.'


if __name__ == '__main__':
    stk_position_analysis()
