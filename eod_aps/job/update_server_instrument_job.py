# -*- coding: utf-8 -*-
import pickle
from eod_aps.job.download_web_etf_job import download_etf_web_job
from eod_aps.job.update_server_db_job import *
from eod_aps.job.download_server_file_job import *
from eod_aps.job.update_local_db_job import update_local_market_job, update_local_etf_job, \
    update_local_market_job_pandas
from eod_aps.job.update_structurefund_etf_job import update_structurefund_etf_index
from eod_aps.job.upload_to_server_job import *
from eod_aps.model.obj_to_sql import to_many_sql
from eod_aps.job.upload_to_server_job import __zip_local_file
from sqlalchemy import and_

trade_servers_list = server_constant.get_trade_servers()
night_session_server_list = server_constant.get_night_session_servers()
etf_base_server = server_constant.get_etf_base_server()

market_servers = server_constant.get_market_servers()
future_market_server = server_constant.get_future_market_server()


def start_update_instrument_backup():
    download_market_file_job(market_servers)
    update_local_market_job()
    upload_market_file_job(trade_servers_list)
    update_server_market_job(trade_servers_list)
    try:
        backup_market_file()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils6.send_email_group_all(u'[Error]备份行情文件异常', error_msg, 'html')


def start_update_instrument():
    download_market_file_job(market_servers)
    update_local_market_job()
    __zip_local_file(DATAFETCHER_MESSAGEFILE_FOLDER, 'market')


def start_update_instrument_pandas():
    download_market_file_job(market_servers)
    update_local_market_job_pandas()
    __zip_local_file(DATAFETCHER_MESSAGEFILE_FOLDER, 'market')


def start_update_instrument_pm():
    download_market_file_job([future_market_server, ])
    update_local_market_job()
    upload_market_file_job(night_session_server_list)
    update_server_market_job(night_session_server_list)


def re_update_instrument():
    rebuild_server_market_files_job(market_servers)
    download_market_file_job(market_servers)
    update_local_market_job()
    upload_market_file_job(trade_servers_list)
    update_server_market_job(trade_servers_list)
    try:
        backup_market_file()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils6.send_email_group_all(u'[Error]备份行情文件异常', error_msg, 'html')


def start_update_etf_backup():
    download_etf_file_job(etf_base_server)
    download_etf_web_job()
    update_local_etf_job()

    upload_etf_file_job(trade_servers_list)
    update_server_etf_job(trade_servers_list)
    update_structurefund_etf_index(trade_servers_list)
    try:
        backup_etf_file()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils6.send_email_group_all(u'[Error]备份ETF文件异常', error_msg, 'html')


def start_update_etf():
    download_etf_file_job(etf_base_server)
    download_etf_web_job()
    update_local_etf_job()


def update_server_instrument_job(server_name_tuple, symbol_type=None):
    server_host = server_constant.get_server_model('host')
    common_session = server_host.get_db_session('common')
    query = common_session.query(Instrument)
    today = date_utils.get_today_str(format_str='%Y-%m-%d')
    obj_list = []
    if not symbol_type:
        for future_db in query.filter(Instrument.del_flag == 0):
            obj_list.append(future_db)
    elif symbol_type == const.INSTRUMENT_TYPE_ENUMS.Future:
        for future_db in query.filter(Instrument.del_flag == 0).filter(
                Instrument.exchange_id.in_([const.EXCHANGE_TYPE_ENUMS.SHF, const.EXCHANGE_TYPE_ENUMS.DCE,
                                            const.EXCHANGE_TYPE_ENUMS.ZCE, const.EXCHANGE_TYPE_ENUMS.CFF,
                                            const.EXCHANGE_TYPE_ENUMS.INE])):
            obj_list.append(future_db)
    else:
        for future_db in query.filter(and_(Instrument.del_flag == 0, Instrument.exchange_id.in_(
                [const.EXCHANGE_TYPE_ENUMS.ANY, const.EXCHANGE_TYPE_ENUMS.HK, const.EXCHANGE_TYPE_ENUMS.CG,
                 const.EXCHANGE_TYPE_ENUMS.CS, const.EXCHANGE_TYPE_ENUMS.FX]))):
            obj_list.append(future_db)

    daily_instrument_obj_list = to_many_sql(Instrument, obj_list, 'common.instrument')
    file_name = 'INSTRUMENT_' + today + '.pickle'
    for ys_file in os.listdir(UPDATE_PRICE_PICKLE):
        os.remove(os.path.join(UPDATE_PRICE_PICKLE, ys_file))
    daily_instrument_obj_list_file = '%s/%s' % (UPDATE_PRICE_PICKLE, file_name)

    if os.path.exists(daily_instrument_obj_list_file):
        os.remove(daily_instrument_obj_list_file)

    with open(daily_instrument_obj_list_file, 'wb') as f:
        pickle.dump(daily_instrument_obj_list, f, True)
    upload_instrument_file_job(server_name_tuple)
    update_server_instrument(server_name_tuple)


if __name__ == '__main__':
    from datetime import datetime

    start_time = datetime.now()
    start_update_instrument_pandas()
    end_time = datetime.now()
    print (end_time - start_time).seconds
