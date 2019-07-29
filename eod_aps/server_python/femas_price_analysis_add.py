# -*- coding: utf-8 -*-
import sys
from datetime import datetime
from eod_aps.model.BaseModel import *
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *

reload(sys)
sys.setdefaultencoding('utf8')

now = datetime.now()
today_str = now.strftime('%Y-%m-%d')

session_common = None
instrument_insert_list = []
shf_db_dict = dict()


def read_price_file_femas(femas_file_path):
    print 'Start read file:', femas_file_path
    fr = open(femas_file_path)
    option_array = []
    future_array = []
    for line in fr.readlines():
        base_model = BaseModel()
        for tempStr in line.split('|')[1].split(','):
            temp_array = tempStr.replace('\n', '').split(':', 1)
            setattr(base_model, temp_array[0].strip(), temp_array[1])
        if 'OnRspQryInstrument' in line:
            product_id = getattr(base_model, 'ProductID', '')
            options_type = getattr(base_model, 'OptionsType', '')
            if (product_id == 'IC') or (product_id == 'IF') or (product_id == 'IH') or (product_id == 'TF') or \
                    (product_id == 'T'):
                future_array.append(base_model)
            elif (options_type == '1') or (options_type == '2'):
                option_array.append(base_model)

    add_future(future_array)  # 新增期货
    add_option(option_array)  # 新增期货期权等


def add_future(message_array):
    global max_instrument_id
    for message_info in message_array:
        ticker = getattr(message_info, 'InstrumentID', '')
        exchange_name = getattr(message_info, 'ExchangeID', '')
        if exchange_name == 'CFFEX':
            exchange_id = 25
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in shf_db_dict:
            continue

        future_db = Instrument()
        max_instrument_id += 1
        future_db.id = max_instrument_id
        future_db.ticker = ticker
        future_db.exchange_id = exchange_id

        instrument_name = getattr(message_info, 'InstrumentName', '')
        future_db.name = instrument_name.replace('中证', 'cs').replace('上证', 'SSE') \
            .replace('股指', 'IDX').replace('期货', 'Future').replace('国债', 'TF').strip()

        future_db.fut_val_pt = getattr(message_info, 'VolumeMultiple', '')
        future_db.max_market_order_vol = getattr(message_info, 'MaxMarketOrderVolume', '')
        future_db.min_market_order_vol = getattr(message_info, 'MinMarketOrderVolume', '')
        future_db.max_limit_order_vol = getattr(message_info, 'MaxLimitOrderVolume', '')
        future_db.min_limit_order_vol = getattr(message_info, 'MinLimitOrderVolume', '')

        product_id = getattr(message_info, 'ProductID', '')
        undl_ticker = ''
        if product_id == 'IF':
            undl_ticker = 'SHSZ300'
        elif product_id == 'IC':
            undl_ticker = 'SH000905'
        elif product_id == 'IH':
            undl_ticker = 'SSE50'
        elif product_id == 'TF':
            undl_ticker = 'TF'
        elif product_id == 'T':
            undl_ticker = 'T'
        future_db.undl_tickers = undl_ticker

        future_db.create_date = getattr(message_info, 'CreateDate', '')
        future_db.expire_date = getattr(message_info, 'ExpireDate', '')

        price_tick = getattr(message_info, 'PriceTick', '')
        future_db.tick_size_table = '0:%s' % (float(price_tick),)

        future_db.market_status = 2
        future_db.type_Id = 1  # future
        future_db.round_lot_size = 1
        future_db.multiplier = 1

        if (product_id == 'T') or (product_id == 'TF'):
            future_db.longmarginratio = 0.015
            future_db.shortmarginratio = 0.015
            future_db.market_sector_id = 5
        else:
            future_db.longmarginratio = 0.1
            future_db.shortmarginratio = 0.1
            future_db.market_sector_id = 6
        instrument_insert_list.append(future_db)
        print 'prepare insert future:', ticker


def add_option(message_array):
    global max_instrument_id
    for message_info in message_array:
        ticker = getattr(message_info, 'InstrumentID', '')
        exchange_name = getattr(message_info, 'ExchangeID', '')
        if exchange_name == 'CFFEX':
            exchange_id = 25
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key in shf_db_dict:
            continue

        option_db = Instrument()
        max_instrument_id += 1
        option_db.id = max_instrument_id
        option_db.ticker = ticker
        option_db.exchange_id = exchange_id

        option_db.fut_val_pt = getattr(message_info, 'VolumeMultiple', '')
        option_db.max_market_order_vol = getattr(message_info, 'MaxMarketOrderVolume', '')
        option_db.min_market_order_vol = getattr(message_info, 'MinMarketOrderVolume', '')
        option_db.max_limit_order_vol = getattr(message_info, 'MaxLimitOrderVolume', '')
        option_db.min_limit_order_vol = getattr(message_info, 'MinLimitOrderVolume', '')

        option_db.strike = getattr(message_info, 'StrikePrice', '')
        option_db.create_date = getattr(message_info, 'CreateDate', '')
        option_db.expire_date = getattr(message_info, 'ExpireDate', '')

        underlying_instr_id = getattr(message_info, 'UnderlyingInstrID', '')
        undl_ticker = underlying_instr_id.replace('HO', 'IH').replace('IO', 'IF')
        undl_ticker = rebuild_undl_ticker(undl_ticker)
        option_db.undl_tickers = undl_ticker

        instrument_name = getattr(message_info, 'InstrumentName', '')
        instrument_name = instrument_name.replace(u'期权', 'OPTION').replace(u'股指', 'IDX').strip()
        option_db.name = instrument_name

        if '-C-' in ticker:
            put_call = 1
        else:
            put_call = 0
        option_db.put_call = put_call

        option_db.market_status_id = 2
        option_db.type_id = 10  # option
        option_db.market_sector_id = 6
        option_db.round_lot_size = 1
        option_db.tick_size_table = '0:0.2'
        option_db.longmarginratio = 1
        option_db.shortmarginratio = 0.15
        option_db.multiplier = 1
        instrument_insert_list.append(option_db)
        print 'prepare insert option:', ticker


def rebuild_undl_ticker(undl_ticker_base):
    query_sql = 'select count(1) from common.instrument where TICKER=%s' % (undl_ticker_base,)
    number = session_common.execute(query_sql).first()[0]
    if number > 0:
        return undl_ticker_base
    else:
        if 'IH' in undl_ticker_base:
            param_key = 'IH'
        elif 'IF' in undl_ticker_base:
            param_key = 'IF'
        query_sql = 'select UNDL_TICKERS from common.instrument where TYPE_ID = 1 and TICKER like %s' % \
                    (param_key + '%',)
        return session_common.execute(query_sql).first()[0]


def get_instrument_max_id():
    global max_instrument_id
    max_instrument_id = session_common.execute('select max(id) from common.instrument').first()[0]


def build_future_db_dict():
    query = session_common.query(Instrument)
    for future in query.filter(Instrument.exchange_id.in_((25,))):
        dict_key = '%s|%s' % (future.ticker, future.exchange_id)
        shf_db_dict[dict_key] = future


def insert_server_db():
    if len(instrument_insert_list) == 0:
        return
    for server_model in server_constant_local.get_all_server():
        server_session = server_model.get_db_session('common')
        for future_db in instrument_insert_list:
            server_session.add(future_db.copy())
        server_session.commit()


if __name__ == '__main__':
    print 'Enter femas_price_analysis_add.'
    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    build_future_db_dict()
    get_instrument_max_id()

    femas_td_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('Femas_TD', today_str)
    for femas_td_file in femas_td_file_list:
        read_price_file_femas('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, femas_td_file))
    session_common.commit()
    server_host.close()
    insert_server_db()
    print 'Exit femas_price_analysis_add.'
