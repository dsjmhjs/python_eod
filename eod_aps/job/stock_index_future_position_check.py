#!/usr/bin/env python
# _*_ coding:utf-8 _*_

from eod_aps.job import *
from eod_aps.model.schema_portfolio import PfPosition, PfAccount
from sqlalchemy import or_

pf_account_info_dict = dict()


def query_pf_position_info(server_name):
    # date_str = date_utils.get_last_trading_day('%Y-%m-%d')
    stock_index_pf_position = []
    server_model = server_constant.get_server_model(server_name)
    session = server_model.get_db_session('portfolio')
    query_sql = 'select max(DATE) from portfolio.pf_position'
    date_str = session.execute(query_sql).first()[0]
    for item in session.query(PfPosition).filter(
            or_(*[PfPosition.symbol.like(k) for k in ['%if%', '%ic%', '%ih%']])).filter(PfPosition.date == date_str):
        stock_index_pf_position.append(
            [pf_account_info_dict[server_name][item.id], item.symbol, int(item.long), int(item.short)])
    stock_index_pf_position = sorted(stock_index_pf_position, key=lambda item: item[0])
    return stock_index_pf_position, date_str


def query_pf_account_info():
    for (server_name, account_list) in const.EOD_CONFIG_DICT['server_pf_account_dict'].items():
        if server_name not in pf_account_info_dict:
            pf_account_info_dict[server_name] = dict()
        for obj in account_list:
            pf_account_info_dict[server_name][obj.id] = obj.fund_name


def stock_index_future_position_check_job():
    query_pf_account_info()
    email_list = []
    for server_name in server_constant.get_all_servers():
        if server_name == 'host':
            continue
        pf_position_list, date_str = query_pf_position_info(server_name)
        if str(date_str) == str(date_utils.get_next_trading_day()):
            email_list.append('<li>server_name[%s] max date: %s</li>' % (server_name, date_str))
        else:
            email_list.append(
                '<li style="color:red">[ERROR]server_name[%s] max date: %s</li>' % (server_name, date_str))
        email_list.extend(email_utils2.list_to_html('fund_name,symbol,long,short', pf_position_list))
        email_list.append('<br>')
    email_utils15.send_email_group_all('股指期货仓位报告', ''.join(email_list), 'html')


if __name__ == '__main__':
    stock_index_future_position_check_job()
