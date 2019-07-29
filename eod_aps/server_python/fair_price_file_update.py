# -*- coding: utf-8 -*-
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.server_python import *


def fair_price_update(filter_date_str):
    if filter_date_str is None or filter_date_str == '':
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    fair_price_file_path = '%s/fair_price_%s.csv' % (DATAFETCHER_MESSAGEFILE_FOLDER, filter_date_str)

    update_sql_list = []
    with open(fair_price_file_path) as fr:
        for line in fr.readlines():
            ticker, inactive_date, fair_price = line.strip().split(',')
            if fair_price == '':
                update_sql = "update instrument set inactive_date= '%s' where ticker='%s'" \
                             % (inactive_date, ticker)
            else:
                update_sql = "update instrument set inactive_date='%s', fair_price= '%s' where ticker='%s'" \
                             % (inactive_date, fair_price, ticker)
            update_sql_list.append(update_sql)
    update_sql_list.insert(0, 'update instrument set inactive_date=NULL, fair_price= NULL \
where type_id = 4 and del_flag = 0')

    server_host = server_constant_local.get_server_model('host')
    server_session = server_host.get_db_session('common')
    for update_sql_str in update_sql_list:
        server_session.execute(update_sql_str)
    server_session.commit()
    server_host.close()


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    fair_price_update(date_str)

