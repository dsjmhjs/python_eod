# -*- coding: utf-8 -*-

import os
import json
from eod_aps.job.account_position_check_job import pf_real_position_check_job
from eod_aps.job import *


def get_strategy_list(main_contrast_old, main_contrast_new):
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select enable,name,instance_name from strategy.strategy_online;"
    query_result = session_strategy.execute(query_sql)
    strategy_name_list = []
    online_strategy_name_list = []
    for result_line in query_result:
        instance_name = result_line[2].lower()
        instance_name_list = instance_name.split(';')
        strategy_name = result_line[1]
        enable = result_line[0]

        if main_contrast_old.lower() in instance_name_list or main_contrast_new.lower() in instance_name_list:
            strategy_name_list.append(strategy_name)
            if enable == 1:
                online_strategy_name_list.append(strategy_name)
    return strategy_name_list, online_strategy_name_list


def get_future_name(ticker_name):
    future_name = ''
    for i in ticker_name:
        if i.isalpha():
            future_name += i.lower()
    return future_name


def instance_check(instance_name, main_contrast_old, main_contrast_new, strategy_name):
    if ';' in instance_name:
        instance_name_list = instance_name.split(';')
        future_name_list = []
        for instance in instance_name_list:
            future_name_list.append(get_future_name(instance))
    else:
        instance_name_list = []
        future_name_list = []
        instance_name_list.append(instance_name)
        future_name_list.append(get_future_name(instance_name))

    Error_flag = False
    if main_contrast_old.lower() in instance_name_list:
        Error_flag = True
    else:
        if get_future_name(main_contrast_new) in future_name_list:
            if main_contrast_new not in instance_name:
                Error_flag = True

    return strategy_name, instance_name, Error_flag


def check_strategy_online(main_contrast_old, main_contrast_new):
    strategy_online_check_result_dict = dict()
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select name,instance_name from strategy.strategy_online;"
    query_result = session_strategy.execute(query_sql)
    for result_line in query_result:
        strategy_name = result_line[0]
        instance_name = result_line[1].lower()
        [strategy_name, instance_name, Error_flag] = instance_check(instance_name, main_contrast_old,
                                                                    main_contrast_new, strategy_name)
        strategy_online_check_result_dict[strategy_name] = [instance_name, Error_flag]
    return strategy_online_check_result_dict


def check_strategy_online_parameter_server(main_contrast_old, main_contrast_new):
    parameter_server_check_result_dict = dict()
    session_strategy = server_host.get_db_session('strategy')
    query_sql = "select name,target_server,parameter_server from strategy.strategy_online " \
                "where strategy_type = 'CTA';"
    query_result = session_strategy.execute(query_sql)
    for result_line in query_result:
        strategy_name = result_line[0]
        target_server_list = result_line[1].lower().split('|')
        parameter_server_list = result_line[2].split('|')
        parameter_server_check_result_server_dict = dict()
        for i in range(len(target_server_list)):
            if i >= len(parameter_server_list):
                continue
            target_server = target_server_list[i]

            parameter_value_dict = json.loads(parameter_server_list[i])
            if 'Target' not in parameter_value_dict:
                continue
            target_name = parameter_value_dict['Target'].lower()
            [strategy_name, instance_name, Error_flag] = instance_check(target_name, main_contrast_old,
                                                                        main_contrast_new, strategy_name)
            parameter_server_check_result_server_dict[target_server] = [instance_name, Error_flag]
            parameter_server_check_result_dict[strategy_name] = parameter_server_check_result_server_dict
    return parameter_server_check_result_dict


def download_target_file(server_model, target_file_folder, download_path, file_name):
    if not os.path.exists(download_path):
        os.mkdir(download_path)

    remote_path = target_file_folder + '/' + file_name
    dest_path = download_path + '/' + file_name.decode('gb2312')
    download_flag = server_model.download_file(remote_path, dest_path)
    return download_flag


def check_config_file(main_contrast_old, main_contrast_new, straetgy_name_list):
    config_file_check_result_dict = dict()
    file_name = 'config.strategyloader.txt'
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        target_config_path = server_model.server_path_dict['tradeplat_project_folder']
        download_flag = download_target_file(server_model, target_config_path, EOD_PROJECT_FOLDER, file_name)
        if download_flag:
            with open(EOD_PROJECT_FOLDER + '/' + file_name, 'rb') as fr:
                strategy_name = ''
                for line in fr.readlines():
                    if '[' in line and 'lib' in line:
                        temp_list = line.split(']')[0].split('.')
                        strategy_name = temp_list[-2] + '.' + temp_list[-1]
                    if 'WatchList' in line:
                        instance_name = line.replace('WatchList = ', '').strip().lower()
                        if strategy_name in straetgy_name_list:
                            [strategy_name, instance_name, error_flag] = instance_check(instance_name, \
                                            main_contrast_old, main_contrast_new, strategy_name)

                            if strategy_name in config_file_check_result_dict:
                                config_file_check_result_dict[strategy_name][server_name] = [instance_name, error_flag]
                            else:
                                config_file_check_result_dict[strategy_name] = dict()
                                config_file_check_result_dict[strategy_name][server_name] = [instance_name, error_flag]
        else:
            custom_log.log_error_job('download error!')
        server_model.close()
    return config_file_check_result_dict


def check_strategy_parameter_target(main_contrast_old, main_contrast_new, strategy_name_list):
    strategy_parameter_target_check_result_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('strategy')
        for strategy_name in strategy_name_list:
            query_sql = "select `VALUE` from strategy.strategy_parameter where `NAME` = '%s'" \
                        " order by time desc limit 1;" % strategy_name
            query_result = session_strategy.execute(query_sql)
            for query_line in query_result:
                strategy_parameter = query_line[0]
                strategy_parameter_dict = json.loads(strategy_parameter.replace('\n', ''))
                if 'Target' not in strategy_parameter_dict:
                    continue
                instance_name = strategy_parameter_dict['Target'].lower()
                [strategy_name, instance_name, Error_flag] = instance_check(instance_name, main_contrast_old,
                                                                            main_contrast_new, strategy_name)
                if strategy_name in strategy_parameter_target_check_result_dict:
                    strategy_parameter_target_check_result_dict[strategy_name][server_name] = [instance_name, Error_flag]
                else:
                    strategy_parameter_target_check_result_dict[strategy_name] = dict()
                    strategy_parameter_target_check_result_dict[strategy_name][server_name] = [instance_name, Error_flag]
        server_model.close()
    return strategy_parameter_target_check_result_dict


def get_strategy_id_list(session_strategy, strategy_name):
    query_sql = "select * from portfolio.pf_account where GROUP_NAME = '%s' and NAME = '%s'" \
                % (strategy_name.split('.')[0], strategy_name.split('.')[1])
    query_result = session_strategy.execute(query_sql)
    strategy_id_list = []
    for result_line in query_result:
        strategy_id_list.append(result_line[0])
    return strategy_id_list


def get_change_month_ticker_dict():
    change_month_ticker_dict = dict()
    today_str = date_utils.get_today_str('%Y-%m-%d')
    change_month_info_file_path = '%s/future_main_contract_change_info.csv' % MAIN_CONTRACT_CHANGE_FILE_FOLDER
    if not os.path.exists(change_month_info_file_path):
        return dict()

    with open(change_month_info_file_path, 'rb') as fr:
        for line in fr.readlines():
            if line.strip() == '':
                continue
            if line.split(',')[0] != today_str:
                continue
            change_month_future = line.split(',')[1].upper()
            change_month_ticker_old = line.split(',')[2].lower()
            change_month_ticker_new = line.split(',')[3].lower()
            change_month_ticker_dict[change_month_future] = [change_month_ticker_old, change_month_ticker_new]
    return change_month_ticker_dict


def position_check(main_contrast_old):
    fund_name_dict = dict()
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        query_sql = "select id,`long`,short,symbol from portfolio.pf_position where symbol like '%s' and date = '%s';" \
                    % (main_contrast_old + '%', date_utils.get_today_str('%Y-%m-%d'))
        query_result = session_portfolio.execute(query_sql)
        id_list = []
        for query_line in query_result:
            symbol_name = query_line[3]
            ticker_name = symbol_name.split(' ')[0]
            if ticker_name.upper() != main_contrast_old.upper():
                continue
            if str(query_line[0]) not in id_list:
                id_list.append(str(query_line[0]))

        if not id_list:
            continue

        query_sql2 = "select id, fund_name from portfolio.pf_account where id in (%s)" % ','.join(id_list)
        query_result2 = session_portfolio.execute(query_sql2)
        fund_name_dict[server_name] = [query_line[1] for query_line in query_result2]
        server_model.close()
    return fund_name_dict


def change_month_check():
    global server_host
    server_host = server_constant.get_server_model('host')

    global server_name_list
    server_name_list = server_constant.get_cta_servers()

    email_list = []
    change_month_ticker_dict = get_change_month_ticker_dict()
    change_month_flag = False
    for [future_name, change_month_ticker_pair] in sorted(change_month_ticker_dict.items()):
        change_month_flag = True
        main_contrast_old = change_month_ticker_pair[0]
        main_contrast_new = change_month_ticker_pair[1]
        [straetgy_name_list, online_strategy_name_list] = get_strategy_list(main_contrast_old, main_contrast_new)
        strategy_online_check_result_dict = check_strategy_online(main_contrast_old, main_contrast_new)
        parameter_server_check_result_dict = check_strategy_online_parameter_server \
            (main_contrast_old, main_contrast_new)
        strategy_parameter_target_check_result_dict = check_strategy_parameter_target \
            (main_contrast_old, main_contrast_new, online_strategy_name_list)
        config_file_check_result_dict = check_config_file \
            (main_contrast_old, main_contrast_new, online_strategy_name_list)

        email_list.append('%s: %s to %s' % (future_name, main_contrast_old, main_contrast_new))

        html_title_list = ['strategy_name', '126_instance']
        for server_name in server_name_list:
            html_title_list.append('126_parameter_server<br>%s' % server_name)
        for server_name in server_name_list:
            html_title_list.append('server_parameter_target<br>%s' % server_name)
        for server_name in server_name_list:
            html_title_list.append('target_config_file<br>%s' % server_name)

        table_list = []
        for strategy_name in straetgy_name_list:
            tr_item_list = ['%s' % strategy_name]
            instance_info = strategy_online_check_result_dict[strategy_name]
            if not instance_info[1]:
                tr_item_list.append('%s' % instance_info[0])
            else:
                tr_item_list.append('%s(Error)' % instance_info[0])

            for server_name in server_name_list:
                if server_name not in parameter_server_check_result_dict[strategy_name]:
                    tr_item_list.append('/')
                    continue
                instance_info = parameter_server_check_result_dict[strategy_name][server_name]
                if not instance_info[1]:
                    tr_item_list.append(instance_info[0])
                else:
                    tr_item_list.append('%s(Error)' % instance_info[0])

            for server_name in server_name_list:
                if strategy_name not in strategy_parameter_target_check_result_dict:
                    tr_item_list.append('/')
                    continue
                if server_name not in strategy_parameter_target_check_result_dict[strategy_name]:
                    tr_item_list.append('/')
                    continue
                instance_info = strategy_parameter_target_check_result_dict[strategy_name][server_name]
                if not instance_info[1]:
                    tr_item_list.append(instance_info[0])
                else:
                    tr_item_list.append('%s(Error)' % instance_info[0])

            for server_name in server_name_list:
                if strategy_name not in config_file_check_result_dict:
                    tr_item_list.append('/')
                    continue
                if server_name not in config_file_check_result_dict[strategy_name]:
                    tr_item_list.append('/')
                    continue
                instance_info = config_file_check_result_dict[strategy_name][server_name]
                if not instance_info[1]:
                    tr_item_list.append(instance_info[0])
                else:
                    tr_item_list.append('%s(Error)' % instance_info[0])
            table_list.append(tr_item_list)
        html_list = email_utils2.list_to_html(','.join(html_title_list), table_list)
        email_list.append(''.join(html_list))

        email_list.append('<font>Position Check:<br>')
        fund_name_dict = position_check(main_contrast_old)

        html_title = 'Position Fund Name'
        table_list = []
        for [server_name, fund_name_list] in fund_name_dict.items():
            for fund_name in fund_name_list:
                table_list.append([server_name + '_' + fund_name,])
        html_list = email_utils2.list_to_html(html_title, table_list)
        email_list.append(''.join(html_list))

    if change_month_flag:
        email_utils2.send_email_group_all('Main Contract Change Check', '\n'.join(email_list), 'html')
        pf_real_position_check_job(server_name_list)
    server_host.close()


if __name__ == "__main__":
    change_month_check()