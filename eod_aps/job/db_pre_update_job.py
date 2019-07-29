# -*- coding: utf-8 -*-
# 数据库预更新操作
# a.更新南华账号的enable值
from eod_aps.model.schema_portfolio import RealAccount
from eod_aps.job import *


# 更新南华数据库的账户enable值
def __update_account_nanhua(server_name, pre_night_market_flag):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        if pre_night_market_flag:
            if 'cff,any' in account_db.allow_targets:
                account_db.enable = 1
            else:
                account_db.enable = 0
        else:
            account_db.enable = 1
        session_portfolio.merge(account_db)
    session_portfolio.commit()
    server_model.close()


# 更新国信数据库的账户enable值
def __update_account_guoxin(server_name, pre_night_market_flag):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    query = session_portfolio.query(RealAccount)
    for account_db in query:
        if pre_night_market_flag:
            if 'PROXY' == account_db.accounttype:
                account_db.enable = 0
            elif 'cff,any' in account_db.allow_targets:
                account_db.enable = 1
            else:
                account_db.enable = 0
        else:
            account_db.enable = 1
        session_portfolio.merge(account_db)

    # 控制oma表是否启动
#     if pre_night_market_flag:
#         enable_value = 0
#     else:
#         enable_value = 1
#     oma_update_sql = "update oma_user a, oma_investor b set a.`ENABLE` = %s where a.user_id = b.user_id \
# and b.INVESTOR_TYPE = 'S'" % enable_value
#     session_portfolio.execute(oma_update_sql)
    session_portfolio.commit()
    server_model.close()


def db_pre_update_job(server_name_list):
    pre_night_market_flag = date_utils.is_pre_night_market()

    for server_name in server_name_list:
        if server_name == 'nanhua':
            __update_account_nanhua(server_name, pre_night_market_flag)
        elif server_name == 'guoxin':
            __update_account_guoxin(server_name, pre_night_market_flag)
    EmailUtils(const.EMAIL_DICT['group2']).send_email_group_all('DB Pre Update Run Over!', '')


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    db_pre_update_job(trade_servers_list)
