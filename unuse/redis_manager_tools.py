# -*- coding: utf-8 -*-
import redis
import datetime
import bcl_pb2
from threading import Timer
from eod_aps.tools.tradeplat_message_tools import *


r = redis.Redis(host='172.16.12.118', port=6379, db=3)
pipeline_redis = r.pipeline()
order_latest_receive_time = None


def __save_instrument_all(socket):
    instrument_dict, market_dict = send_instrument_info_request_msg(socket)
    for (targetid, instrument_message) in instrument_dict.items():
        r.hset("Instrument_all", targetid, instrument_message)


def __save_market(server_name):
    socket = socket_init(server_name)
    instrument_info_msg = send_instrument_info_request_msg2(socket)
    for market_msg in instrument_info_msg.Infos:
        r.hset("Market", market_msg.ID, market_msg.Args)
    latest_receive_time = instrument_info_msg.LatestReceiveTime
    print 'Market LatestReceiveTime:%s, Len:%s' % (__GetDateTime(latest_receive_time),
                                                   len(instrument_info_msg.Infos))
    socket.close()

    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        socket = socket_init(server_name)
        instrument_info_msg = send_instrument_info_request_msg2(socket, latest_receive_time)
        for market_msg in instrument_info_msg.Infos:
            r.hset("Market", market_msg.ID, market_msg.Args)
        latest_receive_time = instrument_info_msg.LatestReceiveTime
        print 'Market LatestReceiveTime:%s, Len:%s' % (__GetDateTime(latest_receive_time),
                                                            len(instrument_info_msg.Infos))
        socket.close()

        validate_number = int(date_utils.get_today_str('%H%M%S'))
        time.sleep(60)


def __save_orders(socket):
    if order_latest_receive_time is None:
        order_info_msg = send_order_info_request_msg2(socket)
    else:
        order_info_msg = send_order_info_request_msg2(socket, order_latest_receive_time)
    global order_latest_receive_time
    order_latest_receive_time = order_info_msg.LatestReceiveTime

    print 'Order LatestReceiveTime:%s, Len:%s' % (__GetDateTime(order_latest_receive_time),
                                                       len(order_info_msg.Orders))


def __save_trades(server_name):
    socket = socket_init(server_name)
    trade_info_msg = send_trade_info_request_msg2(socket)
    for trade_msg_info in trade_info_msg.Trades:
        r.hset("Trades", trade_msg_info.Time.value, trade_msg_info.Trade)
    latest_receive_time = trade_info_msg.LatestReceiveTime
    print 'Trade LatestReceiveTime:%s, Len:%s' % (__GetDateTime(latest_receive_time),
                                                  len(trade_info_msg.Trades))

    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        socket = socket_init(server_name)
        trade_info_msg = send_trade_info_request_msg2(socket, latest_receive_time)
        for trade_msg_info in trade_info_msg.Trades:
            r.hset("Trades", trade_msg_info.Time.value, trade_msg_info.Trade)
        latest_receive_time = trade_info_msg.LatestReceiveTime
        print 'Trade LatestReceiveTime:%s, Len:%s' % (__GetDateTime(latest_receive_time),
                                                           len(trade_info_msg.Trades))
        socket.close()

        validate_number = int(date_utils.get_today_str('%H%M%S'))
        time.sleep(60)


def __save_risk_and_position(server_name):
    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        socket = socket_init(server_name)
        position_risk_msg = send_position_risk_request_msg2(socket)
        for holding_item in position_risk_msg.Holdings:
            r.lpush("Risks", holding_item.Key)
            for risk_msg_info in holding_item.Value:
                r.hset("RisksInfo:%s" % holding_item.Key, risk_msg_info.Key, risk_msg_info.Value)

        for holding_item in position_risk_msg.Holdings2:
            r.lpush("Positions", holding_item.Key)
            for position_msg_info in holding_item.Value:
                r.hset("PositionsInfo:%s" % holding_item.Key, position_msg_info.Key, position_msg_info.Value)
        socket.close()

        print 'Position LatestReceiveTime:%s, Len:%s' % (date_utils.get_today_str('%Y-%m-%d %H:%M:%S'),
                                                           len(position_risk_msg.Holdings2))
        validate_number = int(date_utils.get_today_str('%H%M%S'))
        time.sleep(120)


def __clear_redis():
    r.delete("Trade_Servers")
    r.delete("Instrument_all")
    r.delete("Market")
    r.delete("Order_Relation_Tree")
    r.delete("Orders")
    r.delete("Trades")
    for del_key in r.keys(pattern='Risks*'):
        r.delete(del_key)
    for del_key in r.keys(pattern='Positions*'):
        r.delete(del_key)


def __save_to_redis(socket):
    # __save_market(socket)
    __save_orders(socket)
    # __save_trades(socket)
    # __save_risk_and_position(socket)


def save_market_info(server_name):
    __clear_redis()

    socket = socket_init(server_name)
    __save_instrument_all(socket)

    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        Timer(5, __save_to_redis, (socket,)).start()
        time.sleep(60)


def __GetDateTime(input_value):
    Jan1st1970 = date_utils.string_toDatetime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
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


if __name__ == '__main__':
    save_market_info('guoxin')
