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
from eod_aps.model.instrument import Instrument
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils
from itertools import islice
import redis
import os

multifactor_strategy_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm',
                             'CSI300_MV10Norm', 'ESOP_01']
eventreal_strategy_list = ['Earning', 'Institution', 'Inflow']

trading_time = [['09:30', '15:00'], ]
r = redis.Redis(host='172.16.12.118', port=6379, db=2)
pipeline_redis = r.pipeline()

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
    print '__get_instrument_list'
    __get_instrument_list()
    print '__get_report_file_basket2'
    __get_report_file_basket2()
    print '__get_event_real_basket'
    __get_event_real_basket()
    print '__get_index_basket'
    __get_index_basket()
    print '__get_realposition_basket'
    __get_realposition_basket()

    for (basket_name, position_list) in basket_dict.items():
        redis_title_name = 'market_ret:%s:ticker_list' % basket_name
        if r.llen(redis_title_name) > 0:
            r.ltrim(redis_title_name, 1, 0)

        for position_str in position_list:
            position_items = position_str.split(',')
            if len(position_items) == 2:
                ticker, weight = position_str.split(',')
            elif len(position_items) == 3:
                ticker, weight, volume = position_str.split(',')

            r.lpush(redis_title_name, ticker)
    t = threading.Timer(60, __recv_market_info)
    t.start()


def __get_eventdriven_file_basket():
    redis_title_name = 'market:typelist:eventdriven_file'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    basket_file_floder = '/nas/fifi/EventDriven/result_production_test/Monitor/trade_flow/%s' % date_utils.get_now().strftime(
        "%Y%m%d")
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


def __get_event_real_basket():
    redis_title_name = 'market:typelist:Event_Real_position'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    server_model = ServerConstant().get_server_model('local118')
    session_aggregation = server_model.get_db_session('aggregation')

    total_position_dict = dict()
    total_money = Decimal(0.0)
    for basket_name in eventreal_strategy_list:
        r.lpush(redis_title_name, basket_name)

        ticker_position_dict = dict()
        basket_total_money = Decimal(0.0)
        last_trading_day = date_utils.get_today_str('%Y-%m-%d')
        query_sql = "select a.SYMBOL,sum(a.`LONG`) from aggregation.pf_position a \
    left join aggregation.pf_account b on a.SERVER_NAME = b.SERVER_NAME and a.id = b.id \
    where a.DATE = '%s' and b.FUND_NAME like '%%%s%%' group by a.SYMBOL" % (last_trading_day, basket_name)
        for position_item in session_aggregation.execute(query_sql):
            ticker = position_item[0]
            long_value = Decimal(position_item[1])
            if long_value <= 0:
                continue

            instrument_minbar_info = instrument_dict[ticker]
            ticker_money = long_value * instrument_minbar_info.prev_close
            ticker_position_dict[ticker] = (ticker_money, long_value)

            basket_total_money += ticker_money
            total_money += ticker_money

        basket_dict[basket_name] = []
        for (ticker, ticker_value) in ticker_position_dict.items():
            (ticker_money, long_value) = ticker_value
            ticker_weight = ticker_money / basket_total_money
            basket_dict[basket_name].append('%s,%.4f,%s' % (ticker, ticker_weight, long_value))

            if ticker in total_position_dict:
                (temp_money, temp_long_value) = total_position_dict[ticker]
                temp_money += ticker_money
                temp_long_value += long_value
                total_position_dict[ticker] = (temp_money, temp_long_value)
            else:
                total_position_dict[ticker] = (ticker_money, long_value)

    total_pf_account_name = 'Event_Real_Total'
    basket_dict[total_pf_account_name] = []
    r.lpush(redis_title_name, total_pf_account_name)
    for (ticker, (ticker_money, ticker_long_value)) in total_position_dict.items():
        ticker_weight = ticker_money / total_money
        basket_dict[total_pf_account_name].append('%s,%.4f,%s' % (ticker, ticker_weight, ticker_long_value))


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


def __get_report_file_basket2():
    redis_title_name = 'market:typelist:report_file'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    basket_file_floder = '/dailyjob/Multi_Factor/latest_production_position'
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


def __get_realposition_basket():
    redis_title_name = 'market:typelist:realposition'
    if r.llen(redis_title_name) > 0:
        r.ltrim(redis_title_name, 1, 0)

    server_model = ServerConstant().get_server_model('local118')
    session_aggregation = server_model.get_db_session('aggregation')

    total_position_dict = dict()
    total_money = Decimal(0.0)
    for basket_name in multifactor_strategy_list:
        ticker_position_dict = dict()
        basket_total_money = Decimal(0.0)
        last_trading_day = date_utils.get_today_str('%Y-%m-%d')
        query_sql = "select a.SYMBOL,sum(a.`LONG`) from aggregation.pf_position a \
left join aggregation.pf_account b on a.SERVER_NAME = b.SERVER_NAME and a.id = b.id \
where a.DATE = '%s' and b.FUND_NAME like '%%%s%%' group by a.SYMBOL" % (last_trading_day, basket_name)
        for position_item in session_aggregation.execute(query_sql):
            ticker = position_item[0]
            long_value = Decimal(position_item[1])
            if long_value <= 0:
                continue
            if ticker not in instrument_dict:
                continue

            instrument_minbar_info = instrument_dict[ticker]
            ticker_money = long_value * instrument_minbar_info.prev_close
            ticker_position_dict[ticker] = (ticker_money, long_value)
            basket_total_money += ticker_money
            total_money += ticker_money

        if basket_total_money == 0:
            continue

        r.lpush(redis_title_name, basket_name)

        basket_dict[basket_name] = []
        for (ticker, ticker_value) in ticker_position_dict.items():
            (ticker_money, long_value) = ticker_value
            ticker_weight = ticker_money / basket_total_money
            basket_dict[basket_name].append('%s,%.4f,%s' % (ticker, ticker_weight, long_value))

            if ticker in total_position_dict:
                (temp_money, temp_long_value) = total_position_dict[ticker]
                temp_money += ticker_money
                temp_long_value += long_value
                total_position_dict[ticker] = (temp_money, temp_long_value)
            else:
                total_position_dict[ticker] = (ticker_money, long_value)

    total_pf_account_name = 'MultiFactor_Total'
    basket_dict[total_pf_account_name] = []
    r.lpush(redis_title_name, total_pf_account_name)
    for (ticker, (ticker_money, ticker_long_value)) in total_position_dict.items():
        ticker_weight = ticker_money / total_money
        basket_dict[total_pf_account_name].append('%s,%.4f,%s' % (ticker, ticker_weight, ticker_long_value))


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
            # print "caclution over."
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
