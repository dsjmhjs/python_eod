# -*- coding: utf-8 -*-
from itertools import islice
from eod_aps.model.schema_common import InstrumentCommissionRate
from eod_aps.job import *


def commission_rate_check_job():
    commission_rate_dict = dict()
    with open('F://rate_file//zhongxin.csv', 'rb') as fr:
        for line in islice(fr, 1, None):
            line_item = line.split(',')
            commission_rate_dict[line_item[0]] = (
                float(line_item[1]), float(line_item[2]), float(line_item[3]), float(line_item[4]), float(line_item[5]),
                float(line_item[6]))

    commission_rate_dict_db = dict()
    server_model = server_constant.get_server_model('zhongxin')
    session_common = server_model.get_db_session('common')
    query = session_common.query(InstrumentCommissionRate)
    for icr_db in query:
        commission_rate_dict_db[icr_db.ticker_type] = (float(icr_db.open_ratio_by_money),
                                                       float(icr_db.open_ratio_by_volume),
                                                       float(icr_db.close_ratio_by_money),
                                                       float(icr_db.close_ratio_by_volume),
                                                       float(icr_db.close_today_ratio_by_money),
                                                       float(icr_db.close_today_ratio_by_volume))

    for (ticker_type, items) in commission_rate_dict.items():
        if ticker_type not in commission_rate_dict_db:
            custom_log.log_error_job('miss ticker:%s' % ticker_type)
            continue

        items_db = commission_rate_dict_db[ticker_type]
        if items[0] != items_db[0] or items[1] != items_db[1] or items[2] != items_db[2] or items[3] != items_db[3] or\
                        items[4] != items_db[4] or items[5] != items_db[5]:
            custom_log.log_error_job('error ticker:%s' % ticker_type)
    server_model.close()


if __name__ == '__main__':
    commission_rate_check_job()
