# -*- coding: utf-8-*-
# 计算股票的fair_price = etf_price(当日)/etf_price(停牌日)*stock_price(停牌日)
import os
import json
from decimal import Decimal
from eod_aps.model.schema_common import Instrument
from eod_aps.tools.stock_utils import StockUtils
from eod_aps.job import *

stock_utils = StockUtils()


# 更新停牌时间
def __update_stock_suspend_date():
    suspend_date_list = stock_utils.get_stock_suspend_date_list()
    if len(suspend_date_list) == 0:
        custom_log.log_error_job('[ERROR]Suspend ticker is empty!')
        return

    session_common = server_host.get_db_session('common')
    pre_update_sql = 'update common.instrument set inactive_date = NULL where type_id = 4 and del_flag = 0'
    session_common.execute(pre_update_sql)
    session_common.commit()

    for suspend_date_str in suspend_date_list:
        suspend_date_item = suspend_date_str.split(',')
        update_sql = "update common.instrument set inactive_date='%s' where ticker='%s'" % \
                     (suspend_date_item[1], suspend_date_item[0])
        session_common.execute(update_sql)
    session_common.commit()


def __update_local_fair_price():
    session_common = server_host.get_db_session('common')
    pre_update_sql = 'update common.instrument set fair_price = NULL where type_id = 4 and del_flag = 0'
    session_common.execute(pre_update_sql)

    stock_etf_dict = dict()
    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == Instrument_Type_Enums.MutualFund,
                                      Instrument.pcf != '', Instrument.del_flag == 0):
        pcf_dict = json.loads(instrument_db.pcf)
        if 'Components' not in pcf_dict:
            continue

        for ticker_info_dict in pcf_dict['Components']:
            if ticker_info_dict["AllowCash"] == 'Must':
                continue

            ticker = ticker_info_dict["Ticker"]
            if not ticker.isdigit():
                continue

            stock_etf_dict.setdefault(ticker, []).append(instrument_db)
    custom_log.log_info_job('ETF Include Stock Size:%s' % len(stock_etf_dict))

    fair_price_list = []
    for instrument_db in query.filter(Instrument.type_id == Instrument_Type_Enums.CommonStock,
                                      Instrument.inactive_date != None, Instrument.del_flag == 0):
        if instrument_db.ticker not in stock_etf_dict:
            fair_price_list.append('%s,%s,' % (instrument_db.ticker, instrument_db.inactive_date))
            continue

        inactive_date = instrument_db.inactive_date.strftime("%Y-%m-%d")
        stock_prev_close = Decimal(instrument_db.prev_close)

        weight = 0
        size = 0
        for etf_instrument in stock_etf_dict[instrument_db.ticker]:
            etf_prev_close = Decimal(etf_instrument.prev_close)
            etf_prev_close_inactive = stock_utils.get_prev_close(inactive_date.replace('-', ''), etf_instrument.ticker)
            if etf_prev_close_inactive is None:
                continue
            etf_weight = etf_prev_close / Decimal(etf_prev_close_inactive)
            weight += etf_weight
            size += 1
            if etf_weight > 1.5:
                custom_log.log_error_job('etf_ticker:%s,weight:%s,etf_prev_close:%s,etf_prev_close_inactive:%s' %
                                  (etf_instrument.ticker, etf_weight, etf_prev_close, etf_prev_close_inactive))

        if size > 0:
            fair_price = weight / size * stock_prev_close
            instrument_db.fair_price = fair_price
            fair_price_list.append('%s,%s,%.2f' % (instrument_db.ticker, instrument_db.inactive_date, fair_price))
        else:
            fair_price_list.append('%s,%s,' % (instrument_db.ticker, instrument_db.inactive_date))
            # task_logger.error('Ticker:%s can not calculation fair_price' % instrument_db.ticker)
        session_common.merge(instrument_db)
    session_common.commit()
    session_common.close()

    save_file_folder = '%s/%s' % (PRICE_FILES_BACKUP_FOLDER, date_utils.get_today_str('%Y%m%d'))
    if not os.path.exists(save_file_folder):
        os.mkdir(save_file_folder)
    save_file_path = '%s/fair_price_%s.csv' % (save_file_folder, date_utils.get_today_str('%Y-%m-%d'))
    with open(save_file_path, 'w') as fr:
        fr.write('\n'.join(fair_price_list))


def __upload_fair_price_file(server_name_list):
    today_str = date_utils.get_today_str('%Y%m%d')
    fair_price_file_name = 'fair_price_%s.csv' % date_utils.get_today_str('%Y-%m-%d')
    source_file_path = '%s/%s/%s' % (PRICE_FILES_BACKUP_FOLDER, today_str, fair_price_file_name)

    for server_name in server_name_list:
        server_mode = server_constant.get_server_model(server_name)
        target_file_path = '%s/%s' % (server_mode.server_path_dict['datafetcher_messagefile'], fair_price_file_name)
        server_mode.upload_file(source_file_path, target_file_path)


def __update_server_fair_price(server_name_list):
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        update_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                           '/home/trader/anaconda2/bin/python fair_price_file_update.py']
        server_model.run_cmd_str(';'.join(update_cmd_list))


def fair_price_calculation_job_backup(server_name_list):
    global server_host
    server_host = server_constant.get_server_model('host')

    __update_stock_suspend_date()
    __update_local_fair_price()
    __upload_fair_price_file(server_name_list)
    __update_server_fair_price(server_name_list)
    server_host.close()


def fair_price_calculation_job():
    global server_host
    server_host = server_constant.get_server_model('host')

    __update_stock_suspend_date()
    __update_local_fair_price()
    server_host.close()


if __name__ == '__main__':
    fair_price_calculation_job()
