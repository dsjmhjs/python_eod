# -*- coding: utf-8 -*-
from eod_aps.job import *

date_utils = DateUtils()
email_utils = EmailUtils(const.EMAIL_DICT['group3'])
email_utils14 = EmailUtils(const.EMAIL_DICT['group14'])
server_name_list = ['nanhua', 'zhongxin', 'luzheng']


def get_target_date():
    if not date_utils.is_trading_day():
        target_date = date_utils.get_next_trading_day()
    else:
        if int(date_utils.get_today_str('%H%M%S')) > 161000:
            target_date = date_utils.get_next_trading_day()
        else:
            target_date = date_utils.get_today_str()
    return target_date


def get_strategy_position_clear_flag(strategy_name):
    date_str = get_target_date()
    strategy_position_clear_flag = True
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = 'select id from portfolio.pf_account where group_name = "%s" and `NAME` = "%s";' \
                    % (strategy_name.split('.')[0], strategy_name.split('.')[1])
        query_result = session_portfolio.execute(query_sql)
        id_list = []
        id_flag = False
        for query_line in query_result:
            id_list.append(str(query_line[0]))
            id_flag = True

        if id_flag:
            query_sql2 = 'select id, `LONG`, `SHORT` from portfolio.pf_position where id in (%s) and date = "%s";' \
                         % (','.join(id_list), date_str)
            query_result2 = session_portfolio.execute(query_sql2)
            for query_line in query_result2:
                if query_line[1] != 0 or query_line[2] != 0:
                    strategy_position_clear_flag = False
        server_model.close()
    return strategy_position_clear_flag


def __send_email(strategy_error_list, strategy_waiting_list, strategy_online_list, strategy_offline_list):
    email_list = list()

    email_list.append('Strategy Name Error List:\n')
    email_list.append('\n'.join(strategy_error_list) + '\n\n')

    email_list.append('Strategy Name Waiting List:\n')
    email_list.append('\n'.join(strategy_waiting_list) + '\n\n')

    email_list.append('Strategy Name Online List:\n')
    email_list.append('\n'.join(strategy_online_list) + '\n\n')

    email_list.append('Strategy Name Offline List:\n')
    email_list.append('\n'.join(strategy_offline_list) + '\n\n')

    email_utils.send_email_group_all('Strategy Online Offline Report', '\n'.join(email_list))


def insert_strategy_change_history():
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')

    strategy_error_list = []
    strategy_waiting_list = []
    strategy_online_list = []
    strategy_offline_list = []

    fr = open(STRATEGY_ONLINE_OFFLINE_PATH)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        if ',' not in line:
            continue
        strategy_name_temp = line.strip().split(',')[0]
        strategy_change_state = line.strip().split(',')[1]

        # check strategy name
        query_sql = 'select `NAME` from strategy.strategy_online where `NAME` = "%s"' % strategy_name_temp
        query_result = session_strategy.execute(query_sql)
        strategy_name_flag = False
        for query_line in query_result:
            strategy_name_flag = True
            strategy_name = query_line[0]
        if not strategy_name_flag:
            strategy_error_list.append(strategy_name_temp)
            continue

        # online
        if strategy_change_state == 'online':
            custom_log.log_info_task('online: %s' % strategy_name)
            insert_sql = "INSERT INTO `strategy`.`strategy_change_history` (`id`, `enable`, `name`, `change_type`) " \
                         "VALUES (default, '1', '%s', 'online')" % strategy_name
            session_strategy.execute(insert_sql)
            strategy_online_list.append(strategy_name)
        # offline
        elif strategy_change_state == 'offline':
            strategy_position_clear_flag = get_strategy_position_clear_flag(strategy_name)
            if strategy_position_clear_flag:
                custom_log.log_info_task('offline: %s' % strategy_name)
                insert_sql = "INSERT INTO `strategy`.`strategy_change_history` (`id`, `enable`, `name`, " \
                             "`change_type`) VALUES (default, '1', '%s', 'offline')" % strategy_name
                session_strategy.execute(insert_sql)
                strategy_offline_list.append(strategy_name)
            else:
                strategy_waiting_list.append(strategy_name)
    session_strategy.commit()
    __send_email(strategy_error_list, strategy_waiting_list, strategy_online_list, strategy_offline_list)


def strategy_online_offline_job():
    try:
        insert_strategy_change_history()
        from eod_aps.wsdl.cta_strategy_wsdl.strategy_change_history_tools import strategy_change_history_job
        strategy_change_history_job()
    except:
        import traceback
        email_utils14.send_email_group_all('[Error]Strategy Online Offline', traceback.format_exc(), 'html')
    return 0


if __name__ == '__main__':
    strategy_online_offline_job()
