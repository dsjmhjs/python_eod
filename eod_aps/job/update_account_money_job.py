# -*- coding: utf-8 -*-
import re
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition
from decimal import Decimal
from eod_aps.job import *


server_list = ['huabao', 'zhongxin']
account_money_dict = dict()
ctp_account_list = []


def __build_db_dict(server_model):
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')

    account_dict = dict()
    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        account_dict[account_db.accountid] = account_db
        if 'CTP' == account_db.accounttype:
            ctp_account_list.append(account_db)

    query = session_portfolio.query(AccountPosition)
    for position_db in query.filter(AccountPosition.date == today_filter_str, AccountPosition.symbol == 'CNY'):
        account_db = account_dict[position_db.id]
        key = '%s-%s-%s-%s' % (account_db.accountname, account_db.accounttype, account_db.fund_name, account_db.accountsuffix)
        account_money_dict[key] = position_db.long


def __analysis_log_message(line_str):
    reg = re.compile('^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<log_type>.*)\] Receive command (?P<type>[^ ]*) (?P<account>[^ ]*) (?P<change_money>[^ ]*)')
    regMatch = reg.match(line_str)
    line_dict = regMatch.groupdict()
    return line_dict


def __update_server_file(cmd_str):
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)
        cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                    cmd_str
                    ]
        server_model.run_cmd_str(';'.join(cmd_list))


def update_account_money_job():
    server_model = server_constant.get_server_model('huabao')
    __build_db_dict(server_model)

    today_filter_str = date_utils.get_today_str('%Y-%m-%d')
    monitor_cmd_list = ["cd %s" % server_model.server_path_dict['tradeplat_log_folder'],
                        "grep 'command deposite' screenlog_MainFrame_%s-*" % today_filter_str.replace('-', '')
                ]
    cmd_message_list = server_model.run_cmd_str2(";".join(monitor_cmd_list))

    monitor_cmd_list = ["cd %s" % server_model.server_path_dict['tradeplat_log_folder'],
                        "grep 'command withdraw' screenlog_MainFrame_%s-*" % today_filter_str.replace('-', '')
                ]
    cmd_message_list.extend(server_model.run_cmd_str2(";".join(monitor_cmd_list)))
    if len(cmd_message_list) == 0:
        return

    for return_message_str in cmd_message_list:
        message_dict = __analysis_log_message(return_message_str)
        account_name = message_dict['account']
        if 'CTP' in account_name:
            account_money = account_money_dict[account_name]
            if message_dict['type'] == 'deposite':
                account_money += Decimal(message_dict['change_money'])
            elif message_dict['type'] == 'withdraw':
                account_money -= Decimal(message_dict['change_money'])
            account_money_dict[account_name] = account_money

    temp_cmd_list = []
    for account_db in ctp_account_list:
        key = '%s-%s-%s-%s' % (account_db.accountname, account_db.accounttype, account_db.fund_name, account_db.accountsuffix)
        account_money = account_money_dict[key]
        temp_cmd_list.append('%s=%s' % (account_db.accountid, int(account_money)))
    cmd_str = "echo -e '%s' > account_money.txt" % '\n'.join(temp_cmd_list)

    __update_server_file(cmd_str)


if __name__ == '__main__':
    update_account_money_job()
