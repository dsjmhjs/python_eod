# -*- coding: utf-8 -*-
import six
import zmq
import AllProtoMsg_pb2
import zlib
from eod_aps.model.eod_const import const
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from cfg import custom_log


date_utils = DateUtils()


def socket_init(server_name):
    context = zmq.Context().instance()
    custom_log.log_debug_task("Connecting to server:%s" % server_name)
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b'172.16.11.127-eod-%s' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S.%f"))
    socket.connect(server_constant.get_connect_address(server_name))
    return socket


def send_login_msg(socket, user_name='eod'):
    msg = AllProtoMsg_pb2.LoginMsg()
    msg.UserName = user_name

    custom_log.log_debug_task("Send LoginMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.Login
    msg_list = [six.int2byte(msg_type), msg_str]
    socket.send_multipart(msg_list)

    recv_message = socket.recv_multipart()
    print recv_message


def send_instrument_info_request_msg(socket):
    msg = AllProtoMsg_pb2.InstrumentInfoRequestMsg()
    msg.IsAll = True
    msg.IncludeStaticInfo = True
    custom_log.log_debug_task("Send InstrumentInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.InstrumentInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv InstrumentInfoResponseMsg.")
    instrument_info_msg = AllProtoMsg_pb2.InstrumentInfoResponseMsg()
    instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))

    instrument_msg_dict = dict()
    for instrument_msg in instrument_info_msg.Targets:
        instrument_msg_dict[instrument_msg.id] = instrument_msg

    market_msg_dict = dict()
    for market_msg in instrument_info_msg.Infos:
        market_msg_dict[market_msg.ID] = market_msg
    return instrument_msg_dict, market_msg_dict


def send_instrument_info_request_msg2(socket, last_update=None):
    msg = AllProtoMsg_pb2.InstrumentInfoRequestMsg()
    if last_update is not None:
        msg.IsAll = False
        msg.IncludeStaticInfo = True
        msg.LastUpdate.scale = last_update.scale
        msg.LastUpdate.value = last_update.value
    else:
        msg.IsAll = True
        msg.IncludeStaticInfo = True
    custom_log.log_debug_task("Send InstrumentInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.InstrumentInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv InstrumentInfoResponseMsg.")
    instrument_info_msg = AllProtoMsg_pb2.InstrumentInfoResponseMsg()
    instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return instrument_info_msg


def send_position_risk_request_msg(socket, result_type=1):
    msg = AllProtoMsg_pb2.PositionRiskRequestMsg()
    custom_log.log_debug_task("Send PositionRiskRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.PositionRiskRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv PositionRiskResponseMsg.")
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


def send_position_risk_request_msg2(socket):
    msg = AllProtoMsg_pb2.PositionRiskRequestMsg()
    custom_log.log_debug_task("Send PositionRiskRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.PositionRiskRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv PositionRiskResponseMsg.")
    position_risk_msg = AllProtoMsg_pb2.PositionRiskResponseMsg()
    position_risk_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return position_risk_msg


def send_order_info_request_msg(socket):
    msg = AllProtoMsg_pb2.OrderInfoRequestMsg()
    msg.IsAll = False
    custom_log.log_debug_task("Send OrderInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.OrderInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv OrderInfoResponseMsg.")
    orderinfo_msg = AllProtoMsg_pb2.OrderInfoResponseMsg()
    orderinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))

    order_msg_list = []
    for order_info in orderinfo_msg.Orders:
        order_msg_list.append(order_info)
    return order_msg_list


def send_order_info_request_msg2(socket, last_update=None):
    msg = AllProtoMsg_pb2.OrderInfoRequestMsg()
    if last_update is not None:
        msg.IsAll = False
        msg.LastUpdateTime.scale = last_update.scale
        msg.LastUpdateTime.value = last_update.value
    else:
        msg.IsAll = True

    custom_log.log_debug_task("Send OrderInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.OrderInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv OrderInfoResponseMsg.")
    orderinfo_msg = AllProtoMsg_pb2.OrderInfoResponseMsg()
    orderinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return orderinfo_msg


def send_cancel_order_msg(socket, order_id):
    msg = AllProtoMsg_pb2.CancelOrderMsg()
    msg.SysOrdID = order_id
    # A: mark as canceled
    msg.MarkAsCanceled = True
    # B: mark as fill canceled
    # msg.MarkAsFillCanceled = True

    custom_log.log_debug_task("Send CancelOrderMsg:%s" % msg)
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.CancelOrder
    msg_list = [six.int2byte(msg_type), msg_str]
    socket.send_multipart(msg_list)


def send_trade_info_request_msg(socket):
    msg = AllProtoMsg_pb2.TradeInfoRequestMsg()
    custom_log.log_debug_task("Send TradeInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.TradeInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv TradeInfoResponseMsg.")
    tradeinfo_msg = AllProtoMsg_pb2.TradeInfoResponseMsg()
    tradeinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))

    trade_msg_list = []
    for trade_info in tradeinfo_msg.Trades:
        trade_msg_list.append(trade_info)
    return trade_msg_list


def send_trade_info_request_msg2(socket, last_update=None):
    msg = AllProtoMsg_pb2.TradeInfoRequestMsg()
    if last_update is not None:
        msg.LastUpdateTime.scale = last_update.scale
        msg.LastUpdateTime.value = last_update.value
    custom_log.log_debug_task("Send TradeInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.TradeInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv TradeInfoResponseMsg.")
    tradeinfo_msg = AllProtoMsg_pb2.TradeInfoResponseMsg()
    tradeinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return tradeinfo_msg


def send_strategy_parameter_change_request_msg(socket, strategy_name, location, control_flag):
    msg = AllProtoMsg_pb2.StrategyParameterChangeRequestMsg()
    msg.Name = strategy_name
    msg.IsEnable = control_flag
    msg.Location = location
    custom_log.log_debug_task("Send StrategyParameterChangeRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.StrategyParameterChangeRequest
    msg_list = [six.int2byte(msg_type), msg_str]
    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv Msg.")
    recv_result = six.byte2int(recv_message[0])
    return recv_result


# 修改策略的Used Fund
def send_strategy_account_change_request_msg(socket, strategy_name):
    msg = AllProtoMsg_pb2.StrategyParameterChangeRequestMsg()
    msg.Name = strategy_name
    # msg.Parameter.append('{"Account": "steady_return"}')
    parameter_item = msg.Parameter.add()
    parameter_item.Key = "Account"
    parameter_item.Value = "steady_return;huize01;hongyuan01"

    custom_log.log_debug_task("Send StrategyParameterChangeRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.StrategyParameterChangeRequest
    msg_list = [six.int2byte(msg_type), msg_str]
    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv Msg.")
    recv_result = six.byte2int(recv_message[0])
    return recv_result


def send_serverinfo_request_msg(socket):
    msg = AllProtoMsg_pb2.ServerInfoRequestMsg()

    custom_log.log_debug_task("Send ServerInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.ServerInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv ServerInfoResponseMsg.")
    serverinfo_msg = AllProtoMsg_pb2.ServerInfoResponseMsg()
    serverinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return serverinfo_msg


def send_phone_trade_request_msg(socket, phone_trade_list):
    msg = AllProtoMsg_pb2.PhoneTradeRequestMsg()

    for phone_trade_info in phone_trade_list:
        phone_trade_item = msg.Trades.add()
        phone_trade_item.Fund = phone_trade_info.fund
        phone_trade_item.Strategy1 = phone_trade_info.strategy1
        phone_trade_item.Strategy2 = phone_trade_info.strategy2
        phone_trade_item.Symbol = phone_trade_info.symbol
        phone_trade_item.Direction = phone_trade_info.direction
        phone_trade_item.TradeType = phone_trade_info.tradetype
        phone_trade_item.HedgeFlag = phone_trade_info.hedgeflag
        phone_trade_item.ExPrice = float(phone_trade_info.exprice)
        phone_trade_item.ExQty = int(phone_trade_info.exqty)
        phone_trade_item.IOType = phone_trade_info.iotype

    custom_log.log_debug_task("Send PhoneTradeItem")
    msg_str = msg.SerializeToString()
    msg_type = 29
    msg_list = [six.int2byte(msg_type), msg_str]
    socket.send_multipart(msg_list)


def send_tradeserverinfo_request_msg(socket):
    msg = AllProtoMsg_pb2.TradeServerInfoRequestMsg()
    custom_log.log_debug_task("Send TradeServerInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.TradeServerInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv TradeServerInfoResponseMsg.")
    tradeserverinfo_msg = AllProtoMsg_pb2.TradeServerInfoResponseMsg()
    tradeserverinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return tradeserverinfo_msg


def send_subscribetradeserverinfo_request_msg(socket, tradeserver_info_list):
    msg = AllProtoMsg_pb2.SubscribeTradeServerInfoRequestMsg()

    # msg.TradeServerInfo = tradeserver_info_list
    for tradeserver_info in tradeserver_info_list:
        msg.TradeServerInfo.append(tradeserver_info)

    custom_log.log_debug_task("Send SubscribeTradeServerInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.SubscribeTradeServerInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv SubscribeTradeServerInfoResponseMsg.")
    subscribetradeserverinfo_msg = AllProtoMsg_pb2.SubscribeTradeServerInfoResponseMsg()
    subscribetradeserverinfo_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return subscribetradeserverinfo_msg


def send_strategy_info_request_msg(socket, is_first_request=True):
    msg = AllProtoMsg_pb2.StrategyInfoRequestMsg()
    msg.IsFirstRequest = is_first_request
    custom_log.log_debug_task("Send StrategyInfoRequestMsg")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.StrategyInfoRequest
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv StrategyInfoResponseMsg.")
    strategy_info_response_msg = AllProtoMsg_pb2.StrategyInfoResponseMsg()
    strategy_info_response_msg.ParseFromString(zlib.decompress(recv_message[1]))
    return strategy_info_response_msg


def send_server_parameter_change_request_msg(socket, command, location, service_name):
    msg = AllProtoMsg_pb2.ServerParameterChangeRequestMsg()
    msg.Command = command
    msg.Location = location
    msg.ServiceName = service_name
    custom_log.log_debug_task("Send ServerParameterChangeRequestMsg")
    msg_str = msg.SerializeToString()
    msg_list = [six.int2byte(13), msg_str]

    socket.send_multipart(msg_list)
    recv_message = socket.recv_multipart()
    custom_log.log_debug_task("Recv Msg.")
    recv_result = six.byte2int(recv_message[0])
    return recv_result


def send_new_order(socket, msg):
    custom_log.log_debug_task("Send NewOrder")
    msg_str = msg.SerializeToString()
    msg_type = const.MSG_TYPEID_ENUMS.NewOrder
    msg_list = [six.int2byte(msg_type), msg_str]

    socket.send_multipart(msg_list)


if __name__ == '__main__':
    # socket = socket_init('aggregator')
    # send_login_msg(socket)
    # trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
    # trade_server_info_list = []
    # for trade_server_info in trade_server_info_msg.TradeServerInfo:
    #     trade_server_info_list.append(trade_server_info)
    # send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)
    #
    # strategy_info_response_msg = send_strategy_info_request_msg(socket, False)
    # for strats_info in strategy_info_response_msg.Strats:
    #     print strats_info.Name, strats_info.IsEnabled, strats_info.Location
    pass

