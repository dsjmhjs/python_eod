# -*- coding: utf-8 -*-
import os

from eod_aps.job import *
from itertools import islice
from eod_aps.model.schema_common import Instrument, InstrumentExtend
from eod_aps.tools.instrument_tools import query_instrument_dict

volume_mean_file_template = VOLUME_MEAN_FILE_TEMPLATE


def update_average_volume(filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y%m%d')

    type_list = [Instrument_Type_Enums.CommonStock, Instrument_Type_Enums.MutualFund,
                 Instrument_Type_Enums.MMF, Instrument_Type_Enums.StructuredFund, Instrument_Type_Enums.ReversePurch]
    instrument_dict = query_instrument_dict('host', type_list)

    instrument_extend_dict = dict()
    server_model = server_constant.get_server_model('host')
    session_common = server_model.get_db_session('common')
    query = session_common.query(InstrumentExtend)
    for instrument_extend_db in query:
        instrument_extend_dict[instrument_extend_db.ticker] = instrument_extend_db

    instrument_extend_list = []
    volume_mean_file_path = volume_mean_file_template % filter_date_str
    with open(volume_mean_file_path, 'rb') as fr:
        for line_str in islice(fr, 1, None):
            line_items = line_str.replace('\n', '').split(',')
            if len(line_items) != 2:
                continue
            ticker = line_items[0]
            volume_mean = float(line_items[1])

            if ticker not in instrument_dict:
                custom_log.log_error_job('Error Ticker:%s' % ticker)
                continue

            instrument_db = instrument_dict[ticker]
            if ticker in instrument_extend_dict:
                instrument_extend_db = instrument_extend_dict[ticker]
            else:
                instrument_extend_db = InstrumentExtend()
                instrument_extend_db.ticker = ticker
                instrument_extend_db.exchange_id = instrument_db.exchange_id

            if instrument_extend_db.exchange_id == Exchange_Type_Enums.CG:
                instrument_extend_db.adv20 = volume_mean * 14400
            elif instrument_extend_db.exchange_id == Exchange_Type_Enums.CS:
                instrument_extend_db.adv20 = volume_mean * 14220
            instrument_extend_list.append(instrument_extend_db)

    for instrument_extend_db in instrument_extend_list:
        session_common.merge(instrument_extend_db)
    session_common.commit()
    server_model.close()


def update_volume_tdy(date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y-%m-%d')

    last_date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_filter_str)
    ctp_market_file_name = 'CTP_Market_%s_2.txt' % last_date_str

    ctp_market_file_path = ''
    cta_servers = server_constant.get_cta_servers()

    for server_name in cta_servers:
        ctp_market_file_path = '%s/%s/%s' % (CTP_DATA_BACKUP_PATH, server_name, ctp_market_file_name)
        if os.path.exists(ctp_market_file_path):
            break

    if not os.path.exists(ctp_market_file_path):
        error_message = 'CTP File:%s is Missing!' % ctp_market_file_path
        email_utils2.send_email_group_all('[Warning]CTP File Miss!', error_message, 'html')
        return

    volume_tdy_dict = dict()
    with open(ctp_market_file_path, 'rb') as fr:
        for file_line in fr.readlines():
            line_items = file_line.split(',')
            ticker = line_items[1]
            volume_tdy = line_items[11]
            volume_tdy_dict[ticker] = volume_tdy

    server_model = server_constant.get_server_model('host')
    session_common = server_model.get_db_session('common')
    query = session_common.query(Instrument)

    for instrument_db in query.filter(Instrument.type_id == Instrument_Type_Enums.Future):
        if instrument_db.ticker not in volume_tdy_dict:
            continue
        instrument_db.volume_tdy = volume_tdy_dict[instrument_db.ticker]
    session_common.commit()
    session_common.close()


def instrument_switch_trading_day(server_name_list):
    update_sql = "update common.instrument set volume_tdy=0 where del_flag=0"
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_common = server_model.get_db_session('common')
        session_common.execute(update_sql)
        session_common.commit()


if __name__ == '__main__':
    instrument_switch_trading_day(('guoxin',))
