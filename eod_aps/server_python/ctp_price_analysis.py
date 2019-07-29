# -*- coding: utf-8 -*-
from datetime import datetime
from eod_aps.model.BaseModel import *
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.server_python import *
from eod_aps.model.obj_to_sql import to_many_sql

now = datetime.now()
validate_time = long(now.strftime('%H%M%S'))
instrument_db_dict = dict()


def build_instrument_db_dict():
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.del_flag == 0):
        dict_key = '%s|%s' % (instrument_db.ticker, instrument_db.exchange_id)
        instrument_db_dict[dict_key] = instrument_db


def read_price_file_ctp(ctp_file_path):
    print 'Start read file:', ctp_file_path

    instrument_array = []
    market_array = []
    with open(ctp_file_path) as fr:
        for line in fr.readlines():
            base_model = BaseModel()
            if len(line.strip()) == 0:
                continue
            for tempStr in line.split('|')[1].split(','):
                temp_array = tempStr.replace('\n', '').split(':', 1)
                setattr(base_model, temp_array[0].strip(), temp_array[1])

            if 'OnRspQryInstrument' in line:
                # exchange_id = getattr(base_model, 'ExchangeID', '')
                # product_id = getattr(base_model, 'ProductID', '')
                instrument_array.append(base_model)
            elif 'OnRspQryDepthMarketData' in line:
                market_array.append(base_model)

    update_instrument_info(instrument_array)
    update_market_info(market_array)  # 根据md行情数据更新prev_close


def update_instrument_info(message_array):
    for message_info in message_array:
        ticker = getattr(message_info, 'InstrumentID', '')
        exchange_name = getattr(message_info, 'ExchangeID', '')
        if 'SSE' == exchange_name:
            exchange_id = 18
        elif 'SZE' == exchange_name:
            exchange_id = 19
        elif 'SHFE' == exchange_name:
            exchange_id = 20
        elif 'DCE' == exchange_name:
            exchange_id = 21
        elif 'CZCE' == exchange_name:
            exchange_id = 22
        elif 'CFFEX' == exchange_name:
            exchange_id = 25
        elif 'INE' == exchange_name:
            exchange_id = 35
        else:
            continue

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in instrument_db_dict:
            # print 'error instrument_info key:', dict_key
            continue

        instrument_db = instrument_db_dict[dict_key]
        instrument_db.fut_val_pt = getattr(message_info, 'VolumeMultiple', '')
        instrument_db.max_market_order_vol = getattr(message_info, 'MaxMarketOrderVolume', '')
        instrument_db.min_market_order_vol = getattr(message_info, 'MinMarketOrderVolume', '')
        instrument_db.max_limit_order_vol = getattr(message_info, 'MaxLimitOrderVolume', '')
        instrument_db.min_limit_order_vol = getattr(message_info, 'MinLimitOrderVolume', '')

        instrument_db.longmarginratio = float(getattr(message_info, 'LongMarginRatio', 0))
        if instrument_db.longmarginratio > 10000:
            instrument_db.longmarginratio = 0
        instrument_db.shortmarginratio = float(getattr(message_info, 'ShortMarginRatio', 0))
        if instrument_db.shortmarginratio > 10000:
            instrument_db.shortmarginratio = 0
        instrument_db.longmarginratio_speculation = instrument_db.longmarginratio
        instrument_db.shortmarginratio_speculation = instrument_db.shortmarginratio
        instrument_db.longmarginratio_hedge = instrument_db.longmarginratio
        instrument_db.shortmarginratio_hedge = instrument_db.shortmarginratio
        instrument_db.longmarginratio_arbitrage = instrument_db.longmarginratio
        instrument_db.shortmarginratio_arbitrage = instrument_db.shortmarginratio

        # 期货和期权每日更新到期日信息
        if instrument_db.type_id in (1, 10):
            expire_date_str = getattr(message_info, 'ExpireDate', '')
            instrument_db.expire_date = datetime.strptime(expire_date_str, '%Y%m%d')


def update_market_info(message_array):
    for message_info in message_array:
        ticker = getattr(message_info, 'InstrumentID', '')
        exchange_name = getattr(message_info, 'ExchangeID', '')
        if 'SSE' == exchange_name:
            exchange_id = 18
        elif 'SZE' == exchange_name:
            exchange_id = 19
        elif 'SHFE' == exchange_name:
            exchange_id = 20
        elif 'DCE' == exchange_name:
            exchange_id = 21
        elif 'CZCE' == exchange_name:
            exchange_id = 22
        elif 'CFFEX' == exchange_name:
            exchange_id = 25
        elif 'INE' == exchange_name:
            exchange_id = 35
        else:
            continue

        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in instrument_db_dict:
            continue

        instrument_db = instrument_db_dict[dict_key]
        now_time = long(now.strftime('%H%M%S'))
        if (now_time > 150500) and (now_time < 180000):
            instrument_db.close = getattr(message_info, 'ClosePrice', '')
            instrument_db.close_update_time = datetime.now()
            instrument_db.volume = getattr(message_info, 'Volume', '')
        else:
            instrument_db.prev_close = getattr(message_info, 'PreClosePrice', '')
            instrument_db.prev_settlementprice = getattr(message_info, 'PreSettlementPrice', '')
            instrument_db.uplimit = getattr(message_info, 'UpperLimitPrice', '')
            instrument_db.downlimit = getattr(message_info, 'LowerLimitPrice', '')
            instrument_db.prev_close_update_time = datetime.now()

            if instrument_db.undl_tickers == 'cu':
                if now.strftime('%Y-%m') == instrument_db.expire_date.strftime('%Y-%m'):
                    instrument_db.round_lot_size = 5
        instrument_db.update_date = datetime.now()


def set_main_submain():
    if (validate_time < 150500) or (validate_time > 180000):
        return
    future_undl_tickers_dict = dict()
    for (dict_key, future_db) in instrument_db_dict.items():
        if future_db.exchange_id not in (20, 21, 22):
            continue

        # 判断主次合约前先都置空
        future_db.tranche = None
        if future_db.volume is None:
            future_db.volume = 0

        if future_db.undl_tickers in future_undl_tickers_dict:
            future_undl_tickers_dict[future_db.undl_tickers].append(future_db)
        else:
            future_list = [future_db]
            future_undl_tickers_dict[future_db.undl_tickers] = future_list

    for (dict_key, future_list) in future_undl_tickers_dict.items():
        future_list = sorted(future_list, cmp=lambda x, y: cmp(int(x.volume), int(y.volume)), reverse=True)
        if len(future_list) > 1:
            if int(future_list[0].volume) > 0:
                future_list[0].tranche = 'Main'
            if int(future_list[1].volume) > 0:
                future_list[1].tranche = 'Sub'


def update_db():
    sql_list = to_many_sql(Instrument, instrument_db_dict.values(), 'common.instrument')
    for sql in sql_list:
        session_common.execute(sql)


def ctp_price_analysis(date_str):
    print 'Enter ctp_price_analysis.'
    server_host = server_constant_local.get_server_model('host')

    global session_common
    session_common = server_host.get_db_session('common')

    global filter_date_str
    if date_str is None or date_str == '':
        filter_date_str = now.strftime('%Y-%m-%d')
    else:
        filter_date_str = date_str

    build_instrument_db_dict()
    instrument_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('CTP_INSTRUMENT', filter_date_str)
    market_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('CTP_MARKET', filter_date_str)

    ctp_file_list = []
    ctp_file_list.extend(instrument_file_list)
    ctp_file_list.extend(market_file_list)
    for ctp_file in ctp_file_list:
        read_price_file_ctp('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, ctp_file))

    set_main_submain()
    update_db()
    session_common.commit()
    server_host.close()
    print 'Exit ctp_price_analysis.'


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    ctp_price_analysis(date_str)
    # ctp_price_analysis('2017-11-27')
