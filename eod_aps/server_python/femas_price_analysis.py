# -*- coding: utf-8 -*-
from datetime import datetime
from eod_aps.model.BaseModel import *
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.file_utils import FileUtils
from eod_aps.server_python import *

now = datetime.now()
today_str = now.strftime('%Y-%m-%d')
validate_time = long(now.strftime('%H%M%S'))

session_common = None
future_db_dict = dict()


def read_price_file_femas(femas_file_path):
    print 'Start read file:', femas_file_path
    fr = open(femas_file_path)
    option_array = []
    future_array = []
    instrument_cff_array = []
    market_array = []
    for line in fr.readlines():
        base_model = BaseModel()
        for tempStr in line.split('|')[1].split(','):
            temp_array = tempStr.replace('\n', '').split(':', 1)
            setattr(base_model, temp_array[0].strip(), temp_array[1])
        if 'OnRspQryInstrument' in line:
            product_id = getattr(base_model, 'ProductID', '')
            options_type = getattr(base_model, 'OptionsType', '')
            if product_id in ('IC', 'IF', 'IH', 'T', 'TF', 'TS'):
                future_array.append(base_model)
                instrument_cff_array.append(base_model)
            elif (options_type == '1') or (options_type == '2'):
                option_array.append(base_model)
                instrument_cff_array.append(option_array)
        elif 'OnRtnDepthMarketData' in line:
            market_array.append(base_model)

    update_instrument_cff(future_array)
    update_market_data(market_array)


def update_market_data(message_array):
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_id = 25
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in future_db_dict:
            print 'error future_info key:', dict_key
            continue

        future_db = future_db_dict[dict_key]
        now_time = long(now.strftime('%H%M%S'))
        if now_time < 150500:
            future_db.prev_close = getattr(messageInfo, 'PreClosePrice', '')
            future_db.prev_close_update_time = datetime.now()
        else:
            future_db.close = getattr(messageInfo, 'ClosePrice', '')
            future_db.volume = getattr(messageInfo, 'Volume', '')
            future_db.close_update_time = datetime.now()
        future_db.update_date = datetime.now()


def update_instrument_cff(message_array):
    for messageInfo in message_array:
        ticker = getattr(messageInfo, 'InstrumentID', '')
        exchange_name = getattr(messageInfo, 'ExchangeID', '')
        if exchange_name == 'CFFEX':
            exchange_id = 25
        dict_key = '%s|%s' % (ticker, exchange_id)
        if dict_key not in future_db_dict:
            print 'error future_info key:', dict_key
            continue

        future_db = future_db_dict[dict_key]
        future_db.fut_val_pt = getattr(messageInfo, 'VolumeMultiple', '')
        future_db.max_market_order_vol = getattr(messageInfo, 'MaxMarketOrderVolume', '')
        future_db.min_market_order_vol = getattr(messageInfo, 'MinMarketOrderVolume', '')
        future_db.max_limit_order_vol = getattr(messageInfo, 'MaxLimitOrderVolume', '')
        future_db.min_limit_order_vol = getattr(messageInfo, 'MinLimitOrderVolume', '')


def build_future_db_dict():
    query = session_common.query(Instrument)
    for future in query.filter(Instrument.exchange_id == 25):
        dict_key = '%s|%s' % (future.ticker, future.exchange_id)
        future_db_dict[dict_key] = future


if __name__ == '__main__':
    print 'Enter femas_price_analysis.'
    server_host = server_constant_local.get_server_model('host')
    session_common = server_host.get_db_session('common')
    build_future_db_dict()
    femas_td_file_list = FileUtils(DATAFETCHER_MESSAGEFILE_FOLDER).filter_file('Femas_TD', today_str)
    for femas_td_file in femas_td_file_list:
        read_price_file_femas('%s/%s' % (DATAFETCHER_MESSAGEFILE_FOLDER, femas_td_file))

    for (dict_key, future) in future_db_dict.items():
        session_common.merge(future)
    session_common.commit()
    server_host.close()
    print 'Exit femas_price_analysis.'
