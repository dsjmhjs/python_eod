import six
import zmq
import AllProtoMsg_pb2
import zlib
import datetime
import time
from threading import Timer
import copy
import sys
import bcl_pb2
from decimal import Decimal
from eod_aps.model.instrument import Instrument
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils
from itertools import islice
import pandas as pd
import redis
import os

multifactor_strategy_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm',
                             'CSI300_MV10Norm', 'ESOP_01']
eventreal_strategy_list = ['Earning', 'Institution', 'Inflow']

trading_time = [['09:30', '15:00'], ]
r = redis.Redis(host='172.16.12.118', port=6379, db=4)
pipeline_redis = r.pipeline()

email_message_list = []
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
    for del_key in r.keys(pattern='Positions:*'):
        r.delete(del_key)
    redis_title = 'Strategys'
    if r.hlen(redis_title) > 0:
        r.delete(redis_title)

    print '__build_instrument_dict'
    __build_instrument_dict()

    print '__read_factor_files'
    __read_factor_files()
    print '__get_event_real_basket'
    __get_event_real_basket()
    print '__get_multifactor_basket'
    __get_multifactor_basket()
    print '__get_index_basket'
    __get_index_basket()

    __save_basket_dict()

    validate_number = int(date_utils.get_today_str('%H%M%S'))
    while validate_number <= 180500:
        Timer(5, __calculation_change_ratio, ()).start()
        time.sleep(60)

    # for (basket_name, position_list) in basket_dict.items():
    #     redis_title_name = 'market_ret:%s:ticker_list' % basket_name
    #     if r.llen(redis_title_name) > 0:
    #         r.ltrim(redis_title_name, 1, 0)
    #
    #     for position_str in position_list:
    #         position_items = position_str.split(',')
    #         if len(position_items) == 2:
    #             ticker, weight = position_str.split(',')
    #         elif len(position_items) == 3:
    #             ticker, weight, volume = position_str.split(',')
    #
    #         r.lpush(redis_title_name, ticker)
    # t = threading.Timer(60, __recv_market_info)
    # t.start()


def __calculation_change_ratio():
    start = time.time()
    r3 = redis.Redis(host='172.16.12.118', port=6379, db=3)
    base_instrument_dict = r3.hgetall("Instrument_all")
    instrument_dict = dict()
    for (target_id, instrument_info_str) in base_instrument_dict.items():
        instrument_msg = AllProtoMsg_pb2.Instrument()
        instrument_msg.ParseFromString(instrument_info_str)
        instrument_dict[target_id] = instrument_msg

    base_market_dict = r3.hgetall('Market')
    last_price_dict = dict()
    for (target_id, market_info_str) in base_market_dict.items():
        market_msg = AllProtoMsg_pb2.MarketDataResponseMsg()
        market_msg.ParseFromString(market_info_str)

        instrument_msg = instrument_dict[target_id]
        last_price_dict[instrument_msg.ticker] = market_msg.Args.LastPrice

    temp_message_list = []
    for position_key in r.keys(pattern='Positions:*'):
        position_dict = r.hgetall(position_key)
        for (ticker, ticker_info_str) in position_dict.items():
            (ticker, weight, volume, prev_close, last_price, change_ratio, market_value) = ticker_info_str.split(',')
            if ticker not in last_price_dict:
                temp_message_list.append('Error Strategy:%s,Ticker:%s no last_price.' % (position_key, ticker))
                continue

            last_price = last_price_dict[ticker]
            if last_price == 0:
                temp_message_list.append('Error Strategy:%s,Ticker:%s last_price is 0' % (position_key, ticker))
                continue

            if prev_close == '':
                temp_message_list.append('Error Strategy:%s,Ticker:%s prev_close is null' % (position_key, ticker))
                continue

            try:
                change_ratio = last_price / float(prev_close) - 1
            except Exception, e:
                temp_message_list.append('Strategy:%s,Ticker:%s,prev_close:%s has Exception' %
                                          (position_key, ticker, prev_close))
                change_ratio = 0

            if volume != '':
                market_value = int(volume) * last_price
            rebuild_ticker_info_str = '%s,%s,%s,%s,%s,%s,%s' % \
                                      (ticker, weight, volume, prev_close, last_price, change_ratio, market_value)
            pipeline_redis.hset(position_key, ticker, rebuild_ticker_info_str)
    pipeline_redis.execute()

    if email_message_list:
        print '\n'.join(email_message_list)
        print '\n'.join(temp_message_list)
        del email_message_list[:]

    end = time.time()
    print 'cost:', end - start


def __save_basket_dict():
    for (strategy_name, ticker_info_list) in basket_dict.items():
        if not ticker_info_list:
            email_message_list.append('Strategy:%s len is 0!' % strategy_name)
            continue

        for ticker_info in ticker_info_list:
            ticker_items = ticker_info.split(',')
            pipeline_redis.hset('Positions:%s' % strategy_name, ticker_items[0], ticker_info)
    pipeline_redis.execute()        


def __get_event_real_basket():
    redis_title = 'Strategys'

    server_model = server_constant.get_server_model('local118')
    session_aggregation = server_model.get_db_session('aggregation')

    # TODO
    r.hset(redis_title, 'Event_Real_Total', 'Event_Real,')

    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    for strategy_name in eventreal_strategy_list:
        r.hset(redis_title, strategy_name, 'Event_Real,')

        ticker_position_dict = dict()
        strategy_market_value = Decimal(0.0)
        query_sql = "select a.SYMBOL,sum(a.`LONG`) from aggregation.pf_position a \
    left join aggregation.pf_account b on a.SERVER_NAME = b.SERVER_NAME and a.id = b.id \
    where a.DATE = '%s' and b.FUND_NAME like '%%%s%%' group by a.SYMBOL" % (filter_date_str, strategy_name)
        for position_item in session_aggregation.execute(query_sql):
            ticker = position_item[0]
            long_value = Decimal(position_item[1])
            if long_value <= 0:
                email_message_list.append('Event_Real Strategy:%s,Ticker:%s,Volume:%s Error!' %
                                          (strategy_name, ticker, long_value))
                continue

            if ticker not in instrument_dict:
                email_message_list.append('Event_Real Strategy:%s,Ticker:%s Unfind!' % (strategy_name, ticker))
                continue

            instrument_db = instrument_dict[ticker]
            if instrument_db.prev_close is None:
                email_message_list.append('Event_Real Strategy:%s,Ticker:%s prev_close is null!' %
                                          (strategy_name, ticker))
                continue
            ticker_market_value = long_value * instrument_db.prev_close
            ticker_position_dict[ticker] = (long_value, ticker_market_value, instrument_db.prev_close)
            strategy_market_value += ticker_market_value

        if strategy_market_value == 0:
            email_message_list.append('Factor Strategy:%s Market Value is 0!' % strategy_name)
            continue

        basket_dict[strategy_name] = []
        for (ticker, dict_value) in ticker_position_dict.items():
            (long_value, ticker_market_value, prev_close) = dict_value
            ticker_weight = ticker_market_value / strategy_market_value
            basket_dict[strategy_name].append('%s,%.4f,%s,%.2f,,,' % (ticker, ticker_weight, int(long_value), prev_close))


def __read_factor_files():
    redis_title = 'Strategys'
    # factor_file_floder = '/dailyjob/Multi_Factor/latest_production_position'
    factor_file_floder = 'Z:/dailyjob/Multi_Factor/latest_production_position'
    for file_name in os.listdir(factor_file_floder):
        max_date = None
        factor_name = file_name.split('.')[0]
        basket_dict[factor_name] = []

        input_file = open('%s/%s' % (factor_file_floder, file_name))
        for line in islice(input_file, 1, None):
            line_items = line.replace('\n', '').split(',')
            ticker = line_items[0].replace('"', '').split('.')[0]
            weight = float(line_items[1])
            date_str = line_items[2].replace('"', '')
            if max_date is None or date_str > max_date:
                max_date = date_str

            if ticker not in instrument_dict:
                email_message_list.append('Factor Strategy:%s,Ticker:%s Unfind!' % (file_name, ticker))
                continue

            instrument_db = instrument_dict[ticker]
            if instrument_db.prev_close is None:
                email_message_list.append('Factor Strategy:%s,Ticker:%s prev_close is null!' %
                                          (file_name, ticker))
                continue
            basket_dict[factor_name].append('%s,%.4f,,%.2f,,,' % (ticker, weight, instrument_db.prev_close))
        r.hset(redis_title, factor_name, 'Factor,%s' % max_date)


def __get_index_basket():
    redis_title = 'Strategys'
    for index_name in ('SSE50', 'SHSZ300', 'SH000905'):
        r.hset(redis_title, index_name, 'Index,')
        instrument_db = instrument_dict[index_name]
        basket_dict[index_name] = ['%s,%s,,%s,,,' % (index_name, '1', instrument_db.prev_close)]


def __get_multifactor_basket():
    server_model = server_constant.get_server_model('local118')
    session_aggregation = server_model.get_db_session('aggregation')

    redis_title = 'Strategys'
    # TODO
    r.hset(redis_title, 'MultiFactor_Total', 'MultiFactor,')

    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    for strategy_name in multifactor_strategy_list:
        r.hset(redis_title, strategy_name, 'MultiFactor,')

        ticker_position_dict = dict()
        strategy_market_value = Decimal(0.0)
        query_sql = "select a.SYMBOL,sum(a.`LONG`) from aggregation.pf_position a \
left join aggregation.pf_account b on a.SERVER_NAME = b.SERVER_NAME and a.id = b.id \
where a.DATE = '%s' and b.FUND_NAME like '%%%s%%' group by a.SYMBOL" % (filter_date_str, strategy_name)
        for position_item in session_aggregation.execute(query_sql):
            ticker = position_item[0]
            long_value = Decimal(position_item[1])
            if long_value <= 0:
                email_message_list.append('MultiFactor Strategy:%s,Ticker:%s,Volume:%s Error!' %
                                          (strategy_name, ticker, long_value))
                continue

            if ticker not in instrument_dict:
                email_message_list.append('MultiFactor Strategy:%s,Ticker:%s Unfind!' % (strategy_name, ticker))
                continue

            instrument_db = instrument_dict[ticker]
            if instrument_db.prev_close is None:
                email_message_list.append('MultiFactor Strategy:%s,Ticker:%s prev_close is null!' %
                                          (strategy_name, ticker))
                continue
            ticker_money = long_value * instrument_db.prev_close
            ticker_position_dict[ticker] = (ticker_money, long_value, instrument_db.prev_close)
            strategy_market_value += ticker_money

        if strategy_market_value == 0:
            email_message_list.append('Factor Strategy:%s Market Value is 0!' % strategy_name)
            continue

        basket_dict[strategy_name] = []
        for (ticker, dict_value) in ticker_position_dict.items():
            (ticker_money, long_value, prev_close) = dict_value
            ticker_weight = ticker_money / strategy_market_value
            basket_dict[strategy_name].append('%s,%.4f,%s,%.2f,,,' % (ticker, ticker_weight, int(long_value), prev_close))


def __build_instrument_dict():
    server_model = server_constant.get_server_model('host')
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

    socket.setsockopt(zmq.IDENTITY, b'172.16.12.66-%s_real' % date_utils.get_now().strftime("%Y/%m/%d %H:%M:%S"))
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
            print "Recv Message."
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
    ticker_redis_list = []
    basket_redis_list = []
    for (basket_name, position_list) in basket_dict.items():
        minbar_str = date_utils.get_now().strftime("%Y-%m-%d %H:%M:00")
        basket_ret = Decimal(0.0)

        basket_nominal_amount = Decimal(0.0)
        for position_str in position_list:
            position_items = position_str.split(',')
            if len(position_items) == 2:
                ticker, weight = position_str.split(',')
                volume = 0
            elif len(position_items) == 3:
                ticker, weight, volume = position_str.split(',')

            if ticker not in instrument_market_dict:
                ticker_ret = 0
            else:
                instrument_minbar_info = instrument_market_dict[ticker][-1]
                if instrument_minbar_info.prev_close == 0:
                    continue
                ticker_ret = instrument_minbar_info.last_price / instrument_minbar_info.prev_close - 1
                basket_nominal_amount += Decimal(volume) * instrument_minbar_info.last_price
            basket_ret += Decimal(weight) * ticker_ret

            ticker_redis_key = 'market_ret:%s:%s' % (basket_name, ticker)
            ticker_redis_value = '%s|%.4f' % (weight, ticker_ret)
            ticker_redis_list.append((ticker_redis_key, ticker_redis_value))

        basket_ret = basket_ret * Decimal(100)
        redis_key = 'market_ret:%s:%s' % (basket_name, date_utils.get_now().strftime("%Y-%m-%d"))
        basket_redis_list.append((redis_key, minbar_str, '%.2f' % str(basket_ret)))

        redis_key = 'market_ret:nominal_amount:%s' % basket_name
        r.set(redis_key, basket_nominal_amount)

    for (ticker_redis_key, ticker_redis_value) in ticker_redis_list:
        pipeline_redis.set(ticker_redis_key, ticker_redis_value)

    for (redis_key, minbar_str, minbar_value) in basket_redis_list:
        pipeline_redis.hset(redis_key, minbar_str, minbar_value)
    pipeline_redis.execute()


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


if __name__ == '__main__':
    get_market_info()
    # __read_factor_files()
