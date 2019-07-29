# coding=utf-8
import os
import shutil
import threading
import traceback

from eod_aps.job import *


table_name_list = ['strategy.strategy_parameter', 'strategy.strategy_state', 'om.trade2_history',
                   'portfolio.pf_position', 'portfolio.pf_account', 'common.instrument_commission_rate']


def __export_strategy_table(server_model, full_table_name, filter_date_str, enable_strategy_name_list):
    schema_name, table_name = full_table_name.split('.')
    session = server_model.get_db_session(schema_name)
    strategy_state_list = ['name|time|value', ]
    for enable_strategy_name in enable_strategy_name_list:
        query_sql = "select `NAME`, `TIME`, `VALUE` from %s.%s where `NAME` = '%s' order by time desc limit 1;" \
                    % (schema_name, table_name, enable_strategy_name)
        query_result = session.execute(query_sql)
        for query_line in query_result:
            strategy_state_list.append('%s|%s|%s' % (query_line[0], query_line[1], query_line[2].replace('\n', '')))
    base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_model.name)
    file_path = '%s/%s.csv' % (base_save_folder, full_table_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(strategy_state_list))


def __export_trade_table(server_model, full_table_name, filter_date_str):
    schema_name, table_name = full_table_name.split('.')
    session = server_model.get_db_session(schema_name)

    column_list = []
    query_sql = "select column_name from Information_schema.columns where table_schema='%s' and \
table_Name='%s'" % (schema_name, table_name)
    for result_item in session.execute(query_sql):
        column_list.append(result_item[0])

    strategy_state_list = ['|'.join(column_list)]

    start_time = '%s 20:00:00' % date_utils.get_last_trading_day('%Y-%m-%d', filter_date_str)
    end_time = '%s 20:00:00' % filter_date_str
    query_sql = "select %s from %s where time > '%s' and time < '%s'" % \
                (','.join(column_list), full_table_name, start_time, end_time)
    for result_item in session.execute(query_sql):
        result_str_list = [str(item) for item in result_item]
        strategy_state_list.append('|'.join(result_str_list))

    base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_model.name)
    file_path = '%s/%s.csv' % (base_save_folder, full_table_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(strategy_state_list))


def __export_position_table(server_model, full_table_name, filter_date_str):
    schema_name, table_name = full_table_name.split('.')
    session = server_model.get_db_session(schema_name)

    column_list = []
    query_sql = "select column_name from Information_schema.columns where table_schema='%s' and \
table_Name='%s'" % (schema_name, table_name)
    for result_item in session.execute(query_sql):
        column_list.append(result_item[0])

    strategy_state_list = ['|'.join(column_list)]
    column_list = ['`%s`' % column_name for column_name in column_list]
    query_sql = "select %s from %s where date='%s'" % (','.join(column_list), full_table_name, filter_date_str)
    custom_log.log_debug_job(query_sql)
    for result_item in session.execute(query_sql):
        result_str_list = [str(item) for item in result_item]
        strategy_state_list.append('|'.join(result_str_list))

    base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_model.name)
    file_path = '%s/%s.csv' % (base_save_folder, full_table_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(strategy_state_list))


def __export_account_table(server_model, full_table_name, filter_date_str):
    schema_name, table_name = full_table_name.split('.')
    session = server_model.get_db_session(schema_name)

    column_list = []
    query_sql = "select column_name from Information_schema.columns where table_schema='%s' and \
table_Name='%s'" % (schema_name, table_name)
    for result_item in session.execute(query_sql):
        column_list.append(result_item[0])

    strategy_state_list = ['|'.join(column_list)]
    query_sql = "select %s from %s" % (','.join(column_list), full_table_name)
    custom_log.log_debug_job(query_sql)
    for result_item in session.execute(query_sql):
        result_str_list = [str(item) for item in result_item]
        strategy_state_list.append('|'.join(result_str_list))

    base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_model.name)
    file_path = '%s/%s.csv' % (base_save_folder, full_table_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(strategy_state_list))


def __export_commission_rate_table(server_model, full_table_name, filter_date_str):
    schema_name, table_name = full_table_name.split('.')
    session = server_model.get_db_session(schema_name)

    column_list = []
    query_sql = "select column_name from Information_schema.columns where table_schema='%s' and \
table_Name='%s'" % (schema_name, table_name)
    for result_item in session.execute(query_sql):
        column_list.append(result_item[0])

    strategy_state_item = '|'.join(column_list)
    strategy_state_list = [strategy_state_item]
    query_sql = "select %s from %s" % (','.join(column_list), full_table_name)
    custom_log.log_debug_job(query_sql)
    for result_item in session.execute(query_sql):
        result_str_list = [str(item) for item in result_item]
        strategy_state_item = '|'.join(result_str_list)
        strategy_state_list.append(strategy_state_item)

    base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_model.name)
    file_path = '%s/%s.csv' % (base_save_folder, full_table_name)
    with open(file_path, 'w+') as fr:
        fr.write('\n'.join(strategy_state_list))


def get_enable_strategy_name_list():
    enable_strategy_name_list = []
    server_model_host = server_constant.get_server_model('host')
    session_strategy = server_model_host.get_db_session('strategy')
    query_sql = "select `NAME` from strategy.strategy_online where `ENABLE` = 1 and `STRATEGY_TYPE` = 'CTA';"
    query_result = session_strategy.execute(query_sql)
    for query_line in query_result:
        enable_strategy_name_list.append(query_line[0])
    return enable_strategy_name_list


def __backtest_files_export(server_name, filter_date_str):
    try:
        base_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), server_name)
        if not os.path.exists(base_save_folder):
            os.makedirs(base_save_folder)

        server_model = server_constant.get_server_model(server_name)
        for table_name in table_name_list:
            if table_name in ['strategy.strategy_parameter', 'strategy.strategy_state']:
                __export_strategy_table(server_model, table_name, filter_date_str, enable_strategy_name_list)
            elif table_name == 'om.trade2_history':
                __export_trade_table(server_model, table_name, filter_date_str)
            elif table_name == 'portfolio.pf_position':
                __export_position_table(server_model, table_name, filter_date_str)
            elif table_name == 'portfolio.pf_account':
                __export_account_table(server_model, table_name, filter_date_str)
            elif table_name == 'common.instrument_commission_rate':
                __export_commission_rate_table(server_model, table_name, filter_date_str)
            else:
                raise Exception(u"表名异常")
        server_model.close()
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__backtest_files_export:%s.' % server_name, error_msg)


def backtest_files_export_job(server_name_list, filter_date_str=None):
    if not filter_date_str:
        filter_date_str = date_utils.get_today_str('%Y-%m-%d')

    if not date_utils.is_trading_day(filter_date_str):
        filter_date_str = date_utils.get_next_trading_day('%Y-%m-%d', filter_date_str)

    enable_strategy_name_list = get_enable_strategy_name_list()
    global enable_strategy_name_list

    threads = []
    for server_name in server_name_list:
        t = threading.Thread(target=__backtest_files_export, args=(server_name, filter_date_str))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    source_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), 'guangfa')
    target_save_folder = BACKTEST_BASE_PATH_TEMPLATE % (filter_date_str.replace('-', ''), 'huabao')
    if os.path.exists(target_save_folder):
        shutil.rmtree(target_save_folder, True)
    shutil.copytree(source_save_folder, target_save_folder)


if __name__ == '__main__':
    cta_server_list = server_constant.get_cta_servers()
    backtest_files_export_job(cta_server_list)
