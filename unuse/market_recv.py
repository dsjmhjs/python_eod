# -*- coding: utf-8 -*-
import six
import zmq
import AllProtoMsg_pb2
import zlib
import datetime
import time
import threading
import copy
import sys
from decimal import Decimal
from eod_aps.algo import bcl_pb2
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils
from itertools import islice
import redis
import os

strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm', 'ESOP']
trading_time = [['09:30', '15:00'],]
r = redis.Redis(host='172.16.12.118', port=6379, db=1)

instrument_dict = dict()
basket_dict = dict()
instrument_market_dict = dict()
date_utils = DateUtils()
basket_pnl_dict = dict()
basket_ret_dict = dict()


class Instrument_Minbar_Info:
    ticker = ""
    prev_close = None
    last_price = None
    msg_update_time = None

    def __init__(self, ticker, prev_close):
        self.ticker = ticker
        self.prev_close = prev_close

    def update(self, market_args):
        self.last_price = Decimal(market_args.LastPrice)
        self.msg_update_time = self.__GetDateTime(market_args.UpdateTime)

    def __GetDateTime(self, input_value):
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

    def info_str(self):
        return '%s:%s,%s' % (self.update_time, self.ticker, self.last_price)

    def copy(self):
        return copy.deepcopy(self)


def get_market_info():
    __get_instrument_list()
    __get_report_file_basket()
    __get_eventdriven_file_basket()
    __get_index_basket()
    t = threading.Thread(target=__recv_market_info)
    t.start()


def __get_eventdriven_file_basket():
    redis_title_name = 'market:typelist:eventdriven_file'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    basket_file_floder = '/nas/fifi/EventDriven/result_production_test/Monitor/trade_flow/%s' % date_utils.get_now().strftime("%Y%m%d")
    for file_name in os.listdir(basket_file_floder):
        basket_name = file_name.split('.')[0]
        r.lpush(redis_title_name, basket_name)

        basket_dict[basket_name] = []
        input_file = open('%s/%s' % (basket_file_floder, file_name))
        for line in islice(input_file, 1, None):
            line_items = line.replace('\n', '').split(',')
            ticker = line_items[1].replace('"', '').split('.')[0]
            weight = line_items[2]
            basket_dict[basket_name].append('%s,%s' % (ticker, weight))


def __get_report_file_basket():
    redis_title_name = 'market:typelist:report_file'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    basket_file_floder = '/nas/longling/StockSelection/latest_production_position'
    max_date = None
    for file_name in os.listdir(basket_file_floder):
        basket_name = file_name.split('.')[0]
        r.lpush(redis_title_name, basket_name)

        basket_dict[basket_name] = []
        input_file = open('%s/%s' % (basket_file_floder, file_name))
        for line in islice(input_file, 1, None):
            line_items = line.replace('\n', '').split(',')
            ticker = line_items[0].replace('"', '').split('.')[0]
            weight = line_items[1]
            date_str = line_items[2].replace('"', '')
            basket_dict[basket_name].append('%s,%s' % (ticker, weight))
            if max_date is None or date_str > max_date:
                max_date = date_str
    r.set('market:strategy_date', max_date)


def __get_index_basket():
    redis_title_name = 'market:typelist:index'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    for basket_name in ('SSE50', 'SHSZ300', 'SH000905'):
        r.lpush(redis_title_name, basket_name)
        basket_dict[basket_name] = ['%s,1' % basket_name, ]


def __get_instrument_list():
    server_model = ServerConstant().get_server_model('host')
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_((4, 6))):
        instrument_minbar_info = Instrument_Minbar_Info(instrument_db.ticker, instrument_db.prev_close)
        instrument_dict[instrument_db.ticker] = instrument_minbar_info


def __is_trading_time():
    trading_flag = False

    now_str = date_utils.get_now().strftime("%H:%M")
    print 'now_str:', now_str
    for (str_time, end_time) in trading_time:
        if str_time <= now_str < end_time:
            trading_flag = True
            break
    if now_str > '15:15':
            sys.exit()
    return trading_flag


def __recv_market_info():
    context = zmq.Context().instance()
    print "Connecting to aggregator server"
    socket = context.socket(zmq.DEALER)

    socket.setsockopt(zmq.IDENTITY, b'172.16.11.68-%s' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S"))
    # socket.setsockopt(zmq.RCVTIMEO, 10)

    socket.connect("tcp://172.16.10.188:10000")

    msg = AllProtoMsg_pb2.InstrumentInfoRequestMsg()
    msg.IsAll = True
    msg.IncludeStaticInfo = True
    # msg.LastUpdate = time.time()
    msg_str = msg.SerializeToString()

    msg_list = [six.int2byte(2), msg_str]
    while True:
        if __is_trading_time():
            print "Send InstrumentInfoRequestMsg Message."
            socket.send_multipart(msg_list)
            # Get the reply.
            recv_message = socket.recv_multipart()
            instrument_info_msg = AllProtoMsg_pb2.InstrumentInfoResponseMsg()
            instrument_info_msg.ParseFromString(zlib.decompress(recv_message[1]))

            targets_msg_dict = dict()
            for instrument_msg in instrument_info_msg.Targets:
                targets_msg_dict[instrument_msg.id] = instrument_msg

            for market_msg in instrument_info_msg.Infos:
                __update_market_info(targets_msg_dict, market_msg)

            __update_basket_pnl()
        time.sleep(60)


def __update_basket_pnl():
    for (basket_name, position_list) in basket_dict.items():
        minbar_str = date_utils.get_now().strftime("%Y-%m-%d %H:%M:00")
        basket_ret = Decimal(0.0)
        for position_str in position_list:
            ticker, weight = position_str.split(',')
            if ticker not in instrument_market_dict:
                continue

            instrument_minbar_info = instrument_market_dict[ticker][-1]
            ticker_ret = instrument_minbar_info.last_price / instrument_minbar_info.prev_close - 1
            basket_ret += Decimal(weight) * ticker_ret

        basket_ret = basket_ret * Decimal(100)
        if basket_name in basket_ret_dict:
            basket_ret_dict[basket_name].append([minbar_str, str(basket_ret)])
        else:
            basket_ret_dict[basket_name] = [[minbar_str, str(basket_ret)]]

        redis_key = 'market_ret:%s:%s' % (basket_name, date_utils.get_now().strftime("%Y-%m-%d"))
        r.hset(redis_key, minbar_str, '%.2f' % str(basket_ret))


def __update_market_info(targets_msg_dict, market_msg):
    instrument_info = targets_msg_dict[market_msg.ID]
    ticker = instrument_info.ticker
    if ticker not in instrument_dict:
        return

    market_args = market_msg.Args
    if __GetDateTime(market_args.UpdateTime) == datetime.datetime.strptime("1400-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"):
        return

    instrument_minbar_info = instrument_dict[ticker].copy()
    instrument_minbar_info.update(market_args)
    if ticker in instrument_market_dict:
        last_minbar_info = instrument_market_dict[instrument_info.ticker][-1]
        if last_minbar_info.msg_update_time != instrument_minbar_info.msg_update_time:
            instrument_market_dict[instrument_info.ticker].append(instrument_minbar_info)
    else:
        instrument_market_dict[instrument_info.ticker] = [instrument_minbar_info]

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


def __redis_info():
    r = redis.Redis(host='172.16.12.118', port=6379, db=1)
    # info = r.info()
    # for key in info:
    #     print '%s:%s' % (key, info[key])

    dic = {"2017-04-11 09:00:00": "0", "2017-04-11 09:01:00": "100", "2017-04-11 09:02:00": "-200"}
    strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm']
    r.hmset("Long_IndNorm", dic)

    # print r.hkeys("Long_IndNorm")
    # print r.hvals("Long_IndNorm")
    hh_list = r.hkeys("Long_IndNorm")
    hh_list.sort()
    for time_str in hh_list:
        print time_str, r.hget("Long_IndNorm", time_str)

    # print r.hkeys("Long_IndNorm")
    # r.hset("Long_IndNorm", "2017-04-11 09:03:00", "105")
    # print r.hkeys("Long_IndNorm")
    # print r.hvals("Long_IndNorrm")
    # print(r.hgetall("Long_IndNorm"))

    # r.lpush("Long_MV10Norm", "2017-04-11 09:00:00,0")
    # r.rpush("Long_MV10Norm", "2017-04-11 09:01:00,100", "2017-04-11 09:02:00,-200", "2017-04-11 09:03:00,105")
    # print r.lrange('Long_MV10Norm', start=0, end=-1)


if __name__ == '__main__':
    get_market_info()