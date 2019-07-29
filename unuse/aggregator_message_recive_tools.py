# -*- coding: utf-8 -*-
import six
import zmq
import AllProtoMsg_pb2
import zlib
import bcl_pb2
import datetime

from eod_aps.model.account_position import AccountPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils


date_utils = DateUtils()
server_constant = ServerConstant()

order_status_dict = {'-1': 'None','0': 'New','1': 'PartialFilled','2':'Filled','3':'DoneForDay','4':'Canceled',\
                     '5':'Replace','6':'PendingCancel','7':'Stopped','8':'Rejected','9':'Suspended','10':'PendingNew',\
                     '11':'Calculated','12':'Expired','13':'AcceptedForBidding','14':'PendingReplace',\
                     '15':'EndAsSucceed','16':'Accepted','17':'InternalRejected'}

operation_status_dict = {'-1':'None','0':'New','1':'PartialFilled','2':'Filled','3':'DoneForDay','4':'Canceled',\
                     '5':'Replace','6':'PendingCancel','7':'Stopped','8':'Rejected','9':'Suspended','10':'PendingNew',\
                     '11':'Calculated','12':'Expired','13':'Restated','14':'PendingReplace',\
                     '15':'Accepted','16':'SubmitCancel','17':'SubmitReplace','18':'InternalRejected','-2':'RecoverFILL'}

order_type_dict = {'0':'None','1':'LimitOrder','2':'SingleAlgo','3':'BasketAlgo','4':'EnhancedLimitOrder',\
                     '5':'SpecialLimitOrder','14':'SelfCross'}

direction_dict = {'1':'BUY','-1':'SELL', '0': 'NORM'}


class OrderView:
    id = None
    ticker = None
    order_status = None
    operation_status = None
    qty = 0
    direction = None
    strategy_id =None
    order_account = None
    creation_time = None
    transaction_time = None
    parent_orderid = None
    order_type = 0

    def __init__(self):
        pass

    def print_str(self):
        print 'Creation_Time:%s,Transaction_Time:%s,id:%s,Ticker:%s,Status:%s,Op Status:%s,QTY:%s,Strategy_ID:%s,Account:%s,Parent_orderid:%s,order_type:%s' \
 % (self.creation_time, self.transaction_time, self.id, self.ticker,self.order_status,self.operation_status,self.qty, self.strategy_id, self.order_account, self.parent_orderid, self.order_type)

    def print_reject(self):
        if self.direction == 'BUY':
            print '[%s]%s,%s,%s,%s' % (self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)
        elif self.direction == 'SELL':
            print '[%s]%s,-%s,%s,%s' % (self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)

    def print_none(self):
        if self.direction == 'BUY':
            print '%s,%s,%s,%s,%s,%s' % (self.order_account, self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)
        elif self.direction == 'SELL':
            print '%s,%s,%s,%s,%s,%s' % (self.order_account, self.strategy_id, self.ticker, self.qty, self.transaction_time, self.transaction_time)


def __send_instrument_info_request_msg(socket):
    msg = AllProtoMsg_pb2.InstrumentInfoRequestMsg()
    msg.IsAll = True
    msg.IncludeStaticInfo = True
    msg_str = msg.SerializeToString()
    msg_type = 2
    msg_list = [six.int2byte(msg_type), msg_str]

    print "Send InstrumentInfoRequestMsg."
    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    print "Recv InstrumentInfoResponseMsg."
    instrument_info_msg = AllProtoMsg_pb2.InstrumentInfoResponseMsg()
    instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))

    targets_msg_dict = dict()
    for instrument_msg in instrument_info_msg.Targets:
        targets_msg_dict[instrument_msg.id] = instrument_msg
    return targets_msg_dict


def __send_position_risk_request_msg(socket, result_type=1):
    msg = AllProtoMsg_pb2.PositionRiskRequestMsg()
    msg_str = msg.SerializeToString()
    msg_type = 19
    msg_list = [six.int2byte(msg_type), msg_str]

    print "Send PositionRiskRequestMsg."
    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    print "Recv PositionRiskResponseMsg."
    position_risk_msg = AllProtoMsg_pb2.PositionRiskResponseMsg()
    position_risk_msg.ParseFromString(zlib.decompress(recv_message[1]))

    if result_type == 1:
        risk_msg_list = []
        for holding_item in position_risk_msg.Holdings:
            risk_msg_list.append(holding_item)
        return risk_msg_list
    elif result_type == 2:
        position_msg_list = []
        for holding_item in position_risk_msg.Holdings2:
            position_msg_list.append(holding_item)
        return position_msg_list


def __send_order_info_request_msg(socket):
    msg = AllProtoMsg_pb2.OrderInfoRequestMsg()
    msg.IsAll = False
    msg_str = msg.SerializeToString()
    msg_type = 7
    msg_list = [six.int2byte(msg_type), msg_str]

    print "Send OrderInfoRequestMsg."
    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    print "Recv OrderInfoResponseMsg."
    orderinfo_msg = AllProtoMsg_pb2.OrderInfoResponseMsg()
    orderinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))

    order_msg_list = []
    for order_info in orderinfo_msg.Orders:
        order_msg_list.append(order_info)
    return order_msg_list


def __send_cancel_order_msg(socket, order_id):
    msg = AllProtoMsg_pb2.CancelOrderMsg()
    msg.SysOrdID = order_id
    # # A: mark as canceled
    # msg.MarkAsCanceled = True
    # B: mark as fill canceled
    msg.MarkAsFillCanceled = True
    msg_str = msg.SerializeToString()
    msg_type = 5
    msg_list = [six.int2byte(msg_type), msg_str]
    print "Send CancelOrderMsg."
    socket.send_multipart(msg_list)


def get_connect_address(server_name):
    server_model = server_constant.get_server_model(server_name)
    return server_model.connect_address


def recv_account_info(server_name):
    context = zmq.Context().instance()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'172.16.11.127-%s_real' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S"))
    socket.connect(get_connect_address(server_name))

    instrument_dict = __send_instrument_info_request_msg(socket)
    position_msg_list = __send_position_risk_request_msg(socket, 2)

    today_str = date_utils.get_today_str('%Y-%m-%d')
    account_position_list = []
    for holding_item in position_msg_list:
        account_id = 1
        for value_item in holding_item.Value:
            index_id = value_item.Key
            instrument_msg = instrument_dict[index_id]
            instrument_position_msg = value_item.Value
            account_position = AccountPosition()
            account_position.id = account_id
            account_position.date = today_str
            account_position.symbol = instrument_msg.ticker
            account_position.long = instrument_position_msg.Long
            account_position.long_cost = instrument_position_msg.LongCost
            account_position.long_avail = instrument_position_msg.LongAvailable
            account_position.short = instrument_position_msg.Short
            account_position.short_cost = instrument_position_msg.ShortCost
            account_position.short_avail = instrument_position_msg.ShortAvailable

            account_position.yd_position_long = instrument_position_msg.PrevLong
            account_position.yd_long_remain = instrument_position_msg.YdLongRemain
            account_position.yd_position_short = instrument_position_msg.PrevShort
            account_position.yd_short_remain = instrument_position_msg.YdShortRemain
            account_position_list.append(account_position)
    print len(account_position_list)


def recv_risk_info(server_name):
    context = zmq.Context().instance()
    print "Connecting to aggregator server"
    socket = context.socket(zmq.DEALER)

    socket.setsockopt(zmq.IDENTITY, b'172.16.11.127-%s_real' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S"))
    socket.connect(get_connect_address(server_name))

    risk_msg_list = __send_position_risk_request_msg(socket)
    for risk0 in risk_msg_list:
        strategy_name = risk0.Key
        for temp in risk0.Value:
            ticker_index = temp.Key
            print temp.Value


def __recv_order_info(socket):
    instrument_dict = __send_instrument_info_request_msg(socket)
    order_msg_list = __send_order_info_request_msg(socket)

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
        order_view.creation_time = __GetDateTime(order_info.Order.CreationTime)
        order_view.transaction_time = __GetDateTime(order_info.Order.TransactionTime)
        order_view.parent_orderid = order_info.Order.ParentOrderID
        order_view.order_type = order_type_dict[str(order_info.Order.TypeWire)]
        order_view_list.append(order_view)

    order_view_list.sort(key=lambda obj: obj.transaction_time, reverse=True)
    return order_view_list


def __GetDateTime(input_value):
    Jan1st1970 = datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    value = input_value.value
    if input_value.scale == bcl_pb2.DateTime().TICKS:
        return Jan1st1970 + datetime.timedelta(microseconds=value / 10)
    elif input_value.scale == bcl_pb2.DateTime().MILLISECONDS:
        return Jan1st1970 + datetime.timedelta(milliseconds=value)
    elif input_value.scale == bcl_pb2.DateTime().SECONDS:
        return Jan1st1970 + datetime.timedelta(seconds=value)
    elif input_value.scale == bcl_pb2.DateTime().MINUTES:
        return Jan1st1970 + datetime.timedelta(minutes=value)
    elif input_value.scale == bcl_pb2.DateTime().HOURS:
        return Jan1st1970 + datetime.timedelta(hours=value)
    elif input_value.scale == bcl_pb2.DateTime().DAYS:
        return Jan1st1970 + datetime.timedelta(days=value)
    return Jan1st1970


def __socket_init(server_name):
    context = zmq.Context().instance()
    print "Connecting to server:%s" % server_name
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'172.16.11.127-%s_real' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S"))
    socket.connect(CONNECT_DICT[server_name])
    return socket


def reject_order_query(server_name):
    socket = __socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    i = 0
    output_dict = dict()
    for order_view in order_view_list:
        if order_view.operation_status == 'Rejected' and order_view.order_type == 'LimitOrder':
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
    socket = __socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    none_order_list = []
    for order_view in order_view_list:
        if order_view.operation_status == 'None' and order_view.order_type == 'LimitOrder':
            if order_view.creation_time.strftime("%H:%M:%S") < '14:30:00':
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

    save_path = 'E:/algoFiles/repair'
    for (basket_name, ticker_list) in basket_name_dict.items():
        save_file_path = '%s/%s.txt' % (save_path, basket_name)
        with open(save_file_path, 'w') as fr:
            fr.write('\n'.join(ticker_list))


# 取消状态为none的订单
def none_order_cancel_tools(server_name, fund_name=None):
    socket = __socket_init(server_name)
    order_view_list = __recv_order_info(socket)

    none_order_list = []
    for order_view in order_view_list:
        if order_view.operation_status == 'None' and order_view.order_type == 'LimitOrder':
            none_order_list.append(order_view)
    print len(none_order_list)

    for order_view in none_order_list:
        order_account = order_view.order_account
        if fund_name is not None and fund_name not in order_account:
            continue
        __send_cancel_order_msg(socket, order_view.id)


if __name__ == '__main__':
    recv_account_info('test')
    # none_order_cancel_tools('test', 'balance01')
    # reject_order_query('guoxin')

