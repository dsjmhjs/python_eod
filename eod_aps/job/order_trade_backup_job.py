# -*- coding: utf-8 -*-
# 将order和trade保存至对应的history表中
import threading
import traceback

from eod_aps.job import *


def __order_trade_backup(server_name):
    try:
        server_model = server_constant.get_server_model(server_name)
        session_om = server_model.get_db_session('om')

        order_history_sql = "insert into om.order_history(`ID`,`SYS_ID`,`ACCOUNT`,`HEDGEFLAG`,`SYMBOL`,`DIRECTION`,`TYPE`,\
    `TRADE_TYPE`,`STATUS`,`OP_STATUS`,`PROPERTY`,`CREATE_TIME`,`TRANSACTION_TIME`,`USER_ID`,`STRATEGY_ID`,`PARENT_ORD_ID`\
    ,`QTY`,`PRICE`,`EX_QTY`,`EX_PRICE`,`ALGO_TYPE`) select `ID`,`SYS_ID`,`ACCOUNT`,`HEDGEFLAG`,`SYMBOL`,`DIRECTION`,`TYPE`,\
    `TRADE_TYPE`,`STATUS`,`OP_STATUS`,`PROPERTY`,`CREATE_TIME`,`TRANSACTION_TIME`,`USER_ID`,`STRATEGY_ID`,`PARENT_ORD_ID`,\
    `QTY`,`PRICE`,`EX_QTY`,`EX_PRICE`,`ALGO_TYPE` from om.`order`"
        session_om.execute(order_history_sql)

        trade2_history_sql = "insert into om.trade2_history(ID,TIME,SYMBOL,QTY,PRICE,FEE,TRADE_TYPE,STRATEGY_ID,ACCOUNT,\
    HEDGEFLAG,ORDER_ID,SELF_CROSS,TRADE_ID) select ID,TIME,SYMBOL,QTY,PRICE,FEE,TRADE_TYPE,STRATEGY_ID,ACCOUNT,HEDGEFLAG,ORDER_ID,\
    SELF_CROSS,TRADE_ID from om.trade2"
        session_om.execute(trade2_history_sql)

        order_del_sql = 'delete from om.order'
        session_om.execute(order_del_sql)
        trade2_del_sql = 'delete from om.trade2'
        session_om.execute(trade2_del_sql)
        session_om.commit()
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__order_trade_backup:%s.' % server_name, error_msg)


def order_trade_backup_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__order_trade_backup, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    order_trade_backup_job(('huabao', ))
