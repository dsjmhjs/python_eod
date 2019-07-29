# -*- coding: utf-8 -*-
# 期货账户可用资金报告

from eod_aps.job import *
from eod_aps.model.schema_portfolio import RealAccount, AccountPosition


def future_account_position_report(server_list, filter_date):
    account_money_dict = dict()
    for server_name in server_list:
        server_model = server_constant.get_server_model(server_name)

        future_account_dict = dict()
        session_portfolio = server_model.get_db_session('portfolio')
        for account_db in session_portfolio.query(RealAccount).filter(RealAccount.accounttype == 'CTP'):
            if 'any,future' not in account_db.allow_targets:
                continue
            future_account_dict[account_db.accountid] = account_db

        future_account_id = future_account_dict.keys()
        for account_position_db in session_portfolio.query(AccountPosition) \
                .filter(AccountPosition.date == filter_date,
                        AccountPosition.id.in_(tuple(future_account_id)),
                        AccountPosition.symbol == 'CNY'):
            account_db = future_account_dict[account_position_db.id]
            dict_key = '%s|%s' % (account_db.accountname, account_db.fund_name)
            if dict_key in account_money_dict:
                continue

            long_avail = int(account_position_db.long_avail)
            if long_avail > 300000:
                format_long_avail = '{:,}'.format(int(account_position_db.long_avail))
            else:
                format_long_avail = '{:,}'.format(int(account_position_db.long_avail)) + '(Warning)'
            update_time = str(account_position_db.update_date).split()[1]
            if int(update_time.replace(':', '')) < 153000:
                update_date_str = str(account_position_db.update_date) + '(Error)'
            else:
                update_date_str = account_position_db.update_date
            account_money_dict[dict_key] = (server_name, account_db.fund_name, account_db.accountname,
                                            format_long_avail, update_date_str)
    report_message_list = [y for x, y in account_money_dict.items()]
    report_message_list.sort()
    return report_message_list


def account_position_report_job(server_list):
    filter_date = date_utils.get_today_str('%Y-%m-%d')
    report_message_list = future_account_position_report(server_list, filter_date)
    report_message_title = 'Server,Fund,Account,Long_Avail,Update_Date'
    html_list = email_utils15.list_to_html(report_message_title, report_message_list)
    email_utils15.send_email_group_all(u'期货账户可用资金报告_%s' % filter_date, ''.join(html_list), 'html')


if __name__ == '__main__':
    all_trade_servers = server_constant.get_all_trade_servers()
    account_position_report_job(all_trade_servers)
