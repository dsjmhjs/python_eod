# -*- coding: utf-8 -*-
from eod_aps.model.instrument_commission_rate import InstrumentCommissionRate
from eod_aps.model.server_constans import ServerConstant
from itertools import islice

exchange_file = 'E:/rate_file/nanhua/nanhua.csv'
server_name = 'nanhua'
rate_file_dict = dict()
rate_db_dict = dict()


def __get_rate_db_list():
    query = session_common.query(InstrumentCommissionRate)
    for icr_db in query:
        # rate_db_dict[icr_db.ticker_type] = (float(icr_db.open_ratio_by_money),
        #                                     float(icr_db.open_ratio_by_volume),
        #                                     float(icr_db.close_ratio_by_money),
        #                                     float(icr_db.close_ratio_by_volume),
        #                                     float(icr_db.close_today_ratio_by_money),
        #                                     float(icr_db.close_today_ratio_by_volume))
        rate_db_dict[icr_db.ticker_type] = icr_db


def __get_rate_file_list():
    input_file = open(exchange_file)
    for line in islice(input_file, 1, None):
        line_item = line.split(',')
        ticker = line_item[1].upper()
        if 'IC' == ticker:
            ticker = 'SH000905'
        elif 'IF' == ticker:
            ticker = 'SHSZ300'
        elif 'IH' == ticker:
            ticker = 'SSE50'

        rate_file_dict[ticker] = (
            float(line_item[2]), float(line_item[3]), float(line_item[4]), float(line_item[5]), float(line_item[6]),
            float(line_item[7]))


def __rate_compare():
    for (key, rate_db) in rate_db_dict.items():
        if key.upper() not in rate_file_dict:
            print 'unfind:', key
            continue
        rate_file = rate_file_dict[key.upper()]
        rate_db_str = (float(rate_db.open_ratio_by_money),
                       float(rate_db.open_ratio_by_volume),
                       float(rate_db.close_ratio_by_money),
                       float(rate_db.close_ratio_by_volume),
                       float(rate_db.close_today_ratio_by_money),
                       float(rate_db.close_today_ratio_by_volume))
        if rate_db_str != rate_file:
            print 'ticker:%s|rate_db:%s|reate_file:%s' % (key, rate_db_str, rate_file)
            rate_db.open_ratio_by_money = rate_file[0]
            rate_db.open_ratio_by_volume = rate_file[1]
            rate_db.close_ratio_by_money = rate_file[2]
            rate_db.close_ratio_by_volume = rate_file[3]
            rate_db.close_today_ratio_by_money = rate_file[4]
            rate_db.close_today_ratio_by_volume = rate_file[5]
            session_common.merge(rate_db)


def commission_rat_update():
    __get_rate_file_list()
    __get_rate_db_list()
    __rate_compare()


if __name__ == '__main__':
    host_server_model = ServerConstant().get_server_model(server_name)
    session_common = host_server_model.get_db_session('common')
    commission_rat_update()

    session_common.commit()
