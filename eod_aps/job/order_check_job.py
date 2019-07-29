# -*- coding: utf-8 -*-
# 检查是否存在隔夜单，前日夜盘未成交或未全部成交但是还在队列中的order
import threading
import traceback

from eod_aps.model.schema_portfolio import RealAccount
from eod_aps.model.schema_om import OrderBroker
from eod_aps.job import *


def __order_check_ctp(account_id, server_model):
    now_date_str = date_utils.get_today_str('%Y-%m-%d')
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d', now_date_str)

    overnight_order_list = []
    session_om = server_model.get_db_session('om')
    start_date = last_trading_day + ' 21:00:00'
    query = session_om.query(OrderBroker)
    for order_db in query.filter(OrderBroker.account == account_id,
                                 OrderBroker.insert_time >= start_date):
        if order_db.status == 1 or order_db.status == 3:
            overnight_order_list.append(order_db)
    return overnight_order_list


def __order_check(server_name):
    try:
        error_order_list = []
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query = session_portfolio.query(RealAccount)
        for account_db in query:
            temp_order_list = []
            if account_db.accounttype == 'CTP':
                temp_order_list = __order_check_ctp(account_db.accountid, server_model)
            else:
                pass

            if len(temp_order_list) > 0:
                error_order_list.extend(temp_order_list)

        if len(error_order_list) > 0:
            email_message_list = []
            for order_db in error_order_list:
                email_message_list.append(order_db.to_string())

            title_str = '[Warning]Server:%s Overnight Order List' % server_name
            email_utils4.send_email_group_all(title_str, '\n'.join(email_message_list))
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__order_check:%s.' % server_name, error_msg)


def order_check_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__order_check, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    order_check_job(('test_99',))
