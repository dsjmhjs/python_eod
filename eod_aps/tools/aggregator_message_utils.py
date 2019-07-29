# -*- coding: utf-8 -*-
# 和aggregator进行消息交互类
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.tradeplat_message_tools import *


date_utils = DateUtils()
common_utils = CommonUtils()


class AggregatorMessageUtils(object):
    """
        Aggregator消息管理工具类
    """
    socket = None
    market_latest_receive_time = None
    order_latest_receive_time = None
    trade_latest_receive_time = None

    def __init__(self):
        pass

    def __socket_init(self, server_name):
        context = zmq.Context().instance()
        custom_log.log_info_task("Connecting to server:%s" % server_name)
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, b'172.16.10.126-eod-%s' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S.%f"))
        socket.connect(server_constant.get_connect_address(server_name))
        return socket

    def __send_login_msg(self):
        msg = AllProtoMsg_pb2.LoginMsg()
        msg.UserName = 'eod'
        msg_str = msg.SerializeToString()
        msg_type = const.MSG_TYPEID_ENUMS.Login
        msg_list = [six.int2byte(msg_type), msg_str]
        self.socket.send_multipart(msg_list)

        recv_message = self.socket.recv_multipart()
        print recv_message

    def __subscribe_tradeservers(self):
        # 往aggregator发送tradeserver的订阅消息
        trade_server_info_msg = send_tradeserverinfo_request_msg(self.socket)
        trade_server_info_list = []
        for trade_server_info in trade_server_info_msg.TradeServerInfo:
            trade_server_info_list.append(trade_server_info)
        send_subscribetradeserverinfo_request_msg(self.socket, trade_server_info_list)

    def login_aggregator(self):
        self.socket = self.__socket_init('aggregator')
        # 新版aggregator需要先登陆和订阅交易服务器
        self.__send_login_msg()
        self.__subscribe_tradeservers()

    def query_instrument_dict(self):
        instrument_dict = dict()
        instrument_msg_dict, market_msg_dict = send_instrument_info_request_msg(self.socket)
        for (target_id, instrument_message) in instrument_msg_dict.items():
            instrument_dict[int(target_id)] = instrument_message
        return instrument_dict

    def query_instrument_info_msg2(self):
        if self.market_latest_receive_time is None:
            instrument_info_msg = send_instrument_info_request_msg2(self.socket)
        else:
            instrument_info_msg = send_instrument_info_request_msg2(self.socket, self.market_latest_receive_time)
        self.market_latest_receive_time = instrument_info_msg.LatestReceiveTime
        msg = 'Market LatestReceiveTime:%s, Len:%s' % (common_utils.format_msg_time(self.market_latest_receive_time),
                                                       len(instrument_info_msg.Infos))
        custom_log.log_debug_task(msg)
        return instrument_info_msg

    def query_order_msg(self):
        if self.order_latest_receive_time is None:
            order_info_msg = send_order_info_request_msg2(self.socket)
        else:
            order_info_msg = send_order_info_request_msg2(self.socket, self.order_latest_receive_time)
        self.order_latest_receive_time = order_info_msg.LatestReceiveTime
        msg = 'Order LatestReceiveTime:%s, Len:%s' % \
              (common_utils.format_msg_time(self.order_latest_receive_time), len(order_info_msg.Orders))
        custom_log.log_debug_task(msg)
        return order_info_msg

    def query_trade_msg(self):
        if self.trade_latest_receive_time is None:
            trade_info_msg = send_trade_info_request_msg2(self.socket)
        else:
            trade_info_msg = send_trade_info_request_msg2(self.socket, self.trade_latest_receive_time)
        self.trade_latest_receive_time = trade_info_msg.LatestReceiveTime
        msg = 'Trade LatestReceiveTime:%s, Len:%s' % (common_utils.format_msg_time(self.trade_latest_receive_time),
                                                      len(trade_info_msg.Trades))
        custom_log.log_debug_task(msg)
        return trade_info_msg

    def query_position_risk_msg(self):
        position_risk_msg = send_position_risk_request_msg2(self.socket)
        msg = 'Position LatestReceiveTime:%s, PositionLen:%s, RiskLen:%s' % \
              (date_utils.get_today_str('%Y-%m-%d %H:%M:%S'), len(position_risk_msg.Holdings),
               len(position_risk_msg.Holdings2))
        custom_log.log_debug_task(msg)
        return position_risk_msg

    def query_strategy_info_msg(self):
        strategy_info_response_msg = send_strategy_info_request_msg(self.socket, False)
        return strategy_info_response_msg

    def send_new_order(self, order_message):
        send_new_order(self.socket, order_message)
