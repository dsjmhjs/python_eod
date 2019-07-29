# -*- coding: cp936 -*-

from eod_aps.job import *
from eod_aps.tools.wind_local_tools import w_ys, w_ys_close


def __wind_login():
    global w
    w = w_ys()


def __wind_close():
    w_ys_close()


def get_delist_ticker_list():
    __wind_login()
    delist_ticker_list = w.query_delistsecurity()
    __wind_close()
    return delist_ticker_list


def update_delist_ticker(server_name_list, delist_ticker_list):
    update_item_list = []
    for delist_ticker in delist_ticker_list:
        update_item_list.append("'%s'" % delist_ticker)

    if len(update_item_list) == 0:
        return

    update_sql = "update common.instrument set DEL_FLAG = 1 where exchange_id in (18,19) and DEL_FLAG = 0 \
and ticker in (%s)" % ','.join(update_item_list)

    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_common = server_model.get_db_session('common')
        session_common.execute(update_sql)
        session_common.commit()
        server_model.close()


def update_delist_stock_job(server_name_list):
    delist_ticker_list = get_delist_ticker_list()
    update_delist_ticker(server_name_list, delist_ticker_list)


if __name__ == '__main__':
    update_delist_stock_job(['citics'])
