# -*- coding: utf-8 -*-
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.tradeplat_message_tools import *
import datetime
import bcl_pb2

common_utils = CommonUtils()
order_status_dict = {'-1': 'None', '0': 'New', '1': 'PartialFilled', '2':'Filled', '3':'DoneForDay',
                     '4':'Canceled',  '5':'Replace', '6':'PendingCancel', '7':'Stopped', '8':'Rejected',
                     '9':'Suspended', '10':'PendingNew', '11':'Calculated', '12':'Expired',
                     '13':'AcceptedForBidding', '14':'PendingReplace', '15':'EndAsSucceed',
                     '16':'Accepted', '17':'InternalRejected'}

operation_status_dict = {'-1':'None', '0':'New', '1':'PartialFilled', '2':'Filled', '3':'DoneForDay',
                         '4':'Canceled',  '5':'Replace', '6':'PendingCancel', '7':'Stopped',
                         '8':'Rejected', '9':'Suspended', '10':'PendingNew', '11':'Calculated',
                         '12':'Expired', '13':'Restated', '14':'PendingReplace', '15':'Accepted',
                         '16':'SubmitCancel', '17':'SubmitReplace', '18':'InternalRejected',
                         '-2':'RecoverFILL'}

order_type_dict = {'0': 'None', '1': 'LimitOrder', '2': 'SingleAlgo', '3': 'BasketAlgo',
                   '4': 'EnhancedLimitOrder', '5': 'SpecialLimitOrder', '14': 'SelfCross'}

direction_dict = {'1': 'BUY', '-1': 'SELL', '0': 'NORM'}


class OrderView(object):
    """
        Order数据展示类
    """
    id = None
    ticker = None
    order_status = None
    operation_status = None
    qty = 0
    direction = None
    strategy_id = None
    order_account = None
    creation_time = None
    transaction_time = None
    parent_order_id = None
    order_type = 0

    def __init__(self):
        pass

    def print_str(self):
        print 'Creation_Time:%s,Transaction_Time:%s,id:%s,Ticker:%s,Status:%s,Op Status:%s,QTY:%s,Strategy_ID:%s,Account:%s,Parent_orderid:%s,order_type:%s' \
              % (
              self.creation_time, self.transaction_time, self.id, self.ticker, self.order_status, self.operation_status,
              self.qty, self.strategy_id, self.order_account, self.parent_order_id, self.order_type)

    def print_reject(self):
        if self.direction == 'BUY':
            print '[%s]%s,%s,%s,%s' % (
            self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)
        elif self.direction == 'SELL':
            print '[%s]%s,-%s,%s,%s' % (
            self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)

    def print_none(self):
        if self.direction == 'BUY':
            print '%s,%s,%s,%s,%s,%s' % (
            self.order_account, self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)
        elif self.direction == 'SELL':
            print '%s,%s,%s,%s,%s,%s' % (
            self.order_account, self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)


def __recv_order_info(socket):
    instrument_dict, market_dict = send_instrument_info_request_msg(socket)
    order_msg_list = send_order_info_request_msg(socket)

    order_view_list = []
    for order_info in order_msg_list:
        ticker = None
        if order_info.TargetID in instrument_dict:
            ticker = instrument_dict[order_info.TargetID].ticker

        order_view = OrderView()
        order_view.id = order_info.Order.ID
        order_view.ticker = ticker
        order_view.order_status = order_status_dict[str(order_info.Order.StatusWire)]
        order_view.operation_status = operation_status_dict[str(order_info.Order.OperationStatusWire)]
        order_view.qty = order_info.Order.Qty
        order_view.direction = direction_dict[str(order_info.Order.DirectionWire)]
        order_view.strategy_id = order_info.Order.StrategyID
        order_view.order_account = order_info.Order.OrderAccount
        order_view.creation_time = common_utils.format_msg_time(order_info.Order.CreationTime)
        order_view.transaction_time = common_utils.format_msg_time(order_info.Order.TransactionTime)
        order_view.parent_order_id = order_info.Order.ParentOrderID
        order_view.order_type = order_type_dict[str(order_info.Order.TypeWire)]
        order_view_list.append(order_view)

    order_view_list.sort(key=lambda obj: obj.transaction_time, reverse=True)
    return order_view_list


def reject_order_query(server_name):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    i = 0
    output_dict = dict()
    for order_view in order_view_list:
        if order_view.order_status == 'Rejected' and order_view.order_type == 'LimitOrder':
            i += 1
            key = '%s|%s|%s' % (order_view.ticker, order_view.strategy_id, order_view.order_account)
            if key in output_dict:
                continue
            output_dict[key] = order_view
    print i

    account_rejected_dict = dict()
    for (key_value, order_view) in output_dict.items():
        if order_view.order_account in account_rejected_dict:
            account_rejected_dict[order_view.order_account].append(order_view)
        else:
            account_rejected_dict[order_view.order_account] = [order_view]

    for (account_name, order_list) in account_rejected_dict.items():
        print '------------%s--------------' % account_name
        order_list.sort(key=lambda obj: obj.transaction_time, reverse=True)
        for order_info in order_list:
            start_time_str = order_info.transaction_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
            interval_seconds = date_utils.get_interval_seconds(start_time_str, end_time_str)
            if interval_seconds > 600:
                continue
            order_info.print_reject()


def none_order_query(server_name):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    none_order_list = []
    for order_view in order_view_list:
        if order_view.order_status == 'None' and order_view.order_type == 'LimitOrder':
            if order_view.creation_time.strftime("%H:%M:%S") < '11:00:00':
                none_order_list.append(order_view)

    sell_ticker_dict = dict()
    none_order_dict = dict()
    for order_view in none_order_list:
        key = '%s|%s|%s' % (order_view.order_account, order_view.strategy_id, order_view.ticker)
        if key in none_order_dict:
            if order_view.direction == 'BUY':
                none_order_dict[key] += order_view.qty
            elif order_view.direction == 'SELL':
                none_order_dict[key] -= order_view.qty
        else:
            if order_view.direction == 'BUY':
                none_order_dict[key] = order_view.qty
            elif order_view.direction == 'SELL':
                none_order_dict[key] = -order_view.qty

        if order_view.direction == 'SELL':
            if order_view.ticker in sell_ticker_dict:
                sell_ticker_dict[order_view.ticker] -= order_view.qty
            else:
                sell_ticker_dict[order_view.ticker] = -order_view.qty

    out_list = []
    for (ticker,qty) in sell_ticker_dict.items():
        out_list.append('%s,%s' % (ticker, qty))
    out_list.sort()
    print '\n'.join(out_list)

    basket_name_dict = dict()
    for (key, qty) in none_order_dict.items():
        order_account, strategy_id, ticker = key.split('|')
        account_item = order_account.split('-')
        strategy_item = strategy_id.split('.')
        basket_name = '%s-%s-%s-'% (strategy_item[1], strategy_item[0], account_item[2])
        if basket_name in basket_name_dict:
            basket_name_dict[basket_name].append('%s,%s' % (ticker, qty))
        else:
            basket_name_dict[basket_name] = ['%s,%s' % (ticker, qty)]

    # save_path = 'E:/algoFiles/repair'
    # for (basket_name, ticker_list) in basket_name_dict.items():
    #     save_file_path = '%s/%s.txt' % (save_path, basket_name)
    #     with open(save_file_path, 'w') as fr:
    #         fr.write('\n'.join(ticker_list))


# 查询状态为none的订单
def __query_none_order(socket):
    order_view_list = __recv_order_info(socket)

    none_order_list = []
    for order_view in order_view_list:
        if order_view.order_status == 'Accepted' and order_view.order_type == 'LimitOrder':
            creation_time_str = order_view.creation_time.strftime('%Y-%m-%d %H:%M:%S')
            now_time_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
            interval_seconds = date_utils.get_interval_seconds(creation_time_str, now_time_str)
            if interval_seconds < 10:
                continue
            none_order_list.append(order_view)
    return none_order_list


# 批量取消状态为none的订单
def none_order_cancel_tools(server_name, limit_time=None):
    socket = socket_init(server_name)
    none_order_list = __query_none_order(socket)
    if limit_time:
        today_str = date_utils.get_today_str('%Y-%m-%d')
        limit_date_str = '%s %s' % (today_str, limit_time)
    for order_view in none_order_list:
        # if limit_time and order_view.creation_time > date_utils.string_toDatetime(limit_date_str, '%Y-%m-%d %H:%M:%S'):
        #     continue
        send_cancel_order_msg(socket, order_view.id)


def query_order_constitute(server_name):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    status_constitute_dict = dict()
    for order_view in order_view_list:
        if order_view.order_type != 'LimitOrder':
            continue
        if order_view.order_status in status_constitute_dict:
            status_constitute_dict[order_view.order_status] += 1
        else:
            status_constitute_dict[order_view.order_status] = 1
    return status_constitute_dict


def query_none_order(server_name):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    none_order_list = []
    for order_view in order_view_list:
        if order_view.operation_status == 'None' and order_view.order_type == 'LimitOrder':
            creation_time_str = order_view.creation_time.strftime('%Y-%m-%d %H:%M:%S')
            now_time_str = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
            interval_seconds = date_utils.get_interval_seconds(creation_time_str, now_time_str)
            if interval_seconds < 300:
                continue
            none_order_list.append(order_view)
    return none_order_list


# 批量取消状态为none的订单
def none_order_cancel_tools2(server_name, fund_name=None):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    for order_view in order_view_list:
        if order_view.order_status not in ['Filled', 'Canceled', 'Rejected'] and order_view.order_type == 'LimitOrder':
            order_account = order_view.order_account
            if fund_name is not None and fund_name not in order_account:
                continue

            send_cancel_order_msg(socket, order_view.id)


def query_order(server_name, query_ticker):
    socket = socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    for order_view in order_view_list:
        if order_view.ticker == query_ticker:
            order_view.print_str()


if __name__ == '__main__':
    # print query_none_order('guoxin')
    # none_order_cancel_tools('test_88')
    # query_order_constitute('guoxin')
    none_order_cancel_tools('guoxin')
    # none_order_list = __query_none_order(socket)
    # for order in none_order_list:
    #     print order.print_none()