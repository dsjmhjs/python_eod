# -*- coding: utf-8 -*-
import json
import threading
import traceback

import xlrd
from eod_aps.model.schema_common import Instrument
from eod_aps.job import *


def read_fund_file(fund_file_path):
    ticker_dict = dict()
    data = xlrd.open_workbook(fund_file_path)
    table = data.sheets()[0]  # 通过索引顺序获取
    nrows = table.nrows  # 行数
    for i in range(1, nrows, 1):
        ticker = int(table.cell(i, 0).value)
        mincreationamount = table.cell(i, 2).value
        minredemptionvolume = table.cell(i, 3).value
        if not str(minredemptionvolume).isdigit():
            minredemptionvolume = 1000

        minredemptionleftvolume = table.cell(i, 4).value
        if not str(minredemptionleftvolume).isdigit():
            minredemptionleftvolume = 1000
        ticker_dict[ticker] = (mincreationamount, minredemptionvolume, minredemptionleftvolume)
    return ticker_dict


def __update_instrument_pcf(server_name, ticker_dict):
    try:
        server_model = server_constant.get_server_model(server_name)
        session = server_model.get_db_session('common')
        query = session.query(Instrument)
        instrument_db_dict = {int(x.ticker): x for x in query.filter(Instrument.type_id ==
                                                                     Instrument_Type_Enums.StructuredFund)}

        for (ticker, xls_value) in ticker_dict.items():
            if ticker not in instrument_db_dict:
                custom_log.log_error_job('server_name:%s,error ticker:%s' % (server_name, ticker))
                continue
            (mincreationamount, minredemptionvolume, minredemptionleftvolume) = xls_value
            instrument_db = instrument_db_dict[ticker]

            pcf_dict = json.loads(instrument_db.pcf)
            pcf_dict['MinCreationAmount'] = mincreationamount
            pcf_dict['MinRedemptionVolume'] = minredemptionvolume
            pcf_dict['MinRedemptionLeftVolume'] = minredemptionleftvolume
            instrument_db.pcf = json.dumps(pcf_dict)
            session.merge(instrument_db)
        session.commit()
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__update_instrument_pcf:%s.' % server_name, error_msg)


def update_instrument_pcf_tradingday(trading_day):
    server_host = server_constant.get_server_model('host')
    session = server_host.get_db_session('common')

    type_list = [Instrument_Type_Enums.MutualFund, Instrument_Type_Enums.MMF, Instrument_Type_Enums.StructuredFund]
    query = session.query(Instrument)
    instrument_db_dict = dict()
    for instrument_db in query.filter(Instrument.type_id.in_(type_list)):
        if instrument_db.pcf is not None:
            instrument_db_dict[int(instrument_db.ticker)] = instrument_db

    for (ticker, instrument_db) in instrument_db_dict.items():
        pcf_dict = json.loads(instrument_db.pcf)
        pcf_dict['TradingDay'] = trading_day
        instrument_db.pcf = json.dumps(pcf_dict)
        session.merge(instrument_db)
    session.commit()
    server_host.close()


def update_structurefund_etf_index(server_name_tuple):
    ticker_dict = read_fund_file(STRUCTUREFUND_FILE_PATH)
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__update_instrument_pcf, args=(server_name, ticker_dict))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    update_structurefund_etf_index(('host',))


