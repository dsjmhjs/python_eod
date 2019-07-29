# -*- coding: utf-8 -*-
import os
from eod_aps.model.server_constans import ServerConstant
from cfg import custom_log

server_constant = ServerConstant()
# Mysql本地程序信息和保存信息

server_model = server_constant.get_server_model('host')

sql_folder_path = server_model.server_path_dict['sql_folder_path']
delete_sql_path = server_model.server_path_dict['sql_folder_path'] + '/delete_sql'
local_sql_backup_folder_path = server_model.server_path_dict['local_sql_backup_folder_path']
aggregator_server_name = 'aggregator'

# 需要备份的数据库表信息
backup_table_dict = {'portfolio.account_position': ('DATE', 'Date'),
                     'portfolio.pf_position': ('DATE', 'Date'),
                     'om.order_args': ('UPDATE_TIME', 'Time'),
                     'om.order_broker': ('INSERT_TIME', 'Time'),
                     'om.order_history': ('CREATE_TIME', 'Time'),
                     'om.trade2_broker': ('TIME', 'Time'),
                     'om.trade2_history': ('TIME', 'Time'),
                     'strategy.strategy_parameter': ('TIME', 'Time'),
                     'strategy.strategy_state': ('TIME', 'Time'),
}


def backup_server_schema(server_name, schema_name, end_date):
    server_model = server_constant.get_server_model(server_name)
    sql_file_name = '%s_%s_%s.sql' % (schema_name, server_name, end_date.replace('-', ''))
    sql_file_path = '%s/%s' % (sql_folder_path, sql_file_name)
    cmd = "mysqldump -u%s -P%s -p%s -h %s %s > %s" % \
          (server_model.db_user, server_model.db_port, server_model.db_password, server_model.db_ip, schema_name,
           sql_file_path)
    custom_log.log_info_task(cmd)
    os.system(cmd)
    server_model.close()


def update_to_aggregator_db(server_name, sql_library, end_date):
    server_model_local = server_constant.get_server_model(aggregator_server_name)
    sql_file_name = '%s_%s_%s.sql' % (sql_library, server_name, end_date.replace('-', ''))
    sql_file_path = '%s/%s' % (sql_folder_path, sql_file_name)
    cmd = "mysql -u%s -p%s -h %s %s < %s" % \
          (server_model_local.db_user, server_model_local.db_password, server_model_local.db_ip, server_name,
           sql_file_path)
    custom_log.log_info_task(cmd)
    sub = os.system(cmd)
    custom_log.log_info_task(str(sub))
    server_model_local.close()


def sql_table_backup(server_name, table_name, table_parameters, start_date, end_date):
    server_model_local = server_constant.get_server_model(aggregator_server_name)
    session_server = server_model_local.get_db_session(server_name)

    backup_table_name = table_name.split('.')[1]
    time_column_name, date_time_flag = table_parameters

    create_table_name = '%s.%s_%s_%s' % (server_name, backup_table_name,
                                         start_date.replace('-', ''), end_date.replace('-', ''))
    if date_time_flag == 'Date':
        query_sql = "drop table if exists %s" % create_table_name
        session_server.execute(query_sql)

        query_sql = "create table %s select * from %s.%s where `%s` <= '%s'" % \
                    (create_table_name, server_name, backup_table_name, time_column_name, end_date)
        session_server.execute(query_sql)
    elif date_time_flag == 'Time':
        query_sql = "drop table if exists %s" % create_table_name
        session_server.execute(query_sql)

        query_sql = "create table %s select * from %s.%s where `%s` <= '%s'" % \
                    (create_table_name, server_name, backup_table_name, time_column_name, end_date + ' 23:59:59')
        session_server.execute(query_sql)
    server_model_local.close()


def delete_origin_table(server_name):
    server_model_local = server_constant.get_server_model(aggregator_server_name)
    session_server = server_model_local.get_db_session(server_name)
    query_sql = "select table_name from information_schema.tables where table_schema='%s'" % server_name
    query_result = session_server.execute(query_sql)
    for query_line in query_result:
        table_name = query_line[0]
        if '_20' not in table_name:
            operation_sql = "drop table `%s`" % table_name
            session_server.execute(operation_sql)
    server_model_local.close()


def save_backup_sql_local(server_name, end_date):
    server_model_local = server_constant.get_server_model(aggregator_server_name)
    sql_file_name = 'local_backup_%s_%s.sql' % (server_name, end_date.replace('-', ''))
    sql_file_path = '%s/%s' % (local_sql_backup_folder_path, sql_file_name)
    cmd = "mysqldump.exe -u%s -p%s -h %s %s > %s" % \
          (server_model_local.db_user, server_model_local.db_password, server_model_local.db_ip, server_name,
           sql_file_path)
    custom_log.log_info_task(cmd)
    os.system(cmd)
    server_model_local.close()


def delete_remote_data(table_info_dict, server_name, backup_date):
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    delete_sql_file = os.path.join(delete_sql_path, 'delete_sql_%s_%s.sql' % (backup_date, server_name))

    for remote_sql_table_name, table_parameters in table_info_dict.items():
        time_column_name, date_time_flag = table_parameters
        if date_time_flag == 'Date':
            delete_sql = "delete from %s where `%s` <= '%s';" % \
                         (remote_sql_table_name, time_column_name, backup_date)
        elif date_time_flag == 'Time':
            delete_sql = "delete from %s where `%s` <= '%s';" % \
                         (remote_sql_table_name, time_column_name, backup_date + ' 23:59:59')
        custom_log.log_info_task(delete_sql)
        session_portfolio.execute(delete_sql)
        with open(delete_sql_file, 'a') as f:
            f.write(delete_sql)
    session_portfolio.commit()
    server_model.close()


def db_backup(server_name_list, table_info_dict, start_date, end_date):
    # 构建backup_sql_name_list和backup_table_name_list
    backup_schema_name_list = []
    for remote_sql_table_name in table_info_dict.keys():
        backup_schema_name = remote_sql_table_name.split('.')[0]
        backup_schema_name_list.append(backup_schema_name)

    # 从服务端将数据库存成文件，并写入到本地数据库
    for server_name in server_name_list:
        for backup_schema_name in set(backup_schema_name_list):
            custom_log.log_info_task('Deal With Schema:%s' % backup_schema_name)
            backup_server_schema(server_name, backup_schema_name, end_date)
            update_to_aggregator_db(server_name, backup_schema_name, end_date)

    # 将数据库表按给定的日期进行分割，分别存到新的数据表中
    for server_name in server_name_list:
        for table_name, table_parameters in table_info_dict.items():
            sql_table_backup(server_name, table_name, table_parameters, start_date, end_date)

    # 删除本地原始下载的表，用_20关键字来筛选的，2100年以后会有问题:)
    for server_name in server_name_list:
        delete_origin_table(server_name)

    # 将数据库备份到本地
    # for server_name in server_name_list:
    #     save_backup_sql_local(server_name, end_date)

    # 数据库删除对应的内容,并保存至文件用于手动执行托管服务器数据库的更新
    for server_name in server_name_list:
        delete_remote_data(table_info_dict, server_name, end_date)


def db_backup_tools(servers_list):
    # import datetime
    # format_str = '%Y-%m-%d'
    # last_month_start = datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).strftime(format_str)
    # last_month_end = (datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)).strftime(format_str)
    last_month_start = '2019-01-01'
    last_month_end = '2019-01-31'
    table_info_dict = backup_table_dict
    db_backup(servers_list, table_info_dict, last_month_start, last_month_end)


def custom_backup_tools():
    servers_list = ['huabao', ]
    last_month_start = '2018-09-01'
    last_month_end = '2018-10-31'
    table_info_dict = {'om.order_args': ('UPDATE_TIME', 'Time')}
    db_backup(servers_list, table_info_dict, last_month_start, last_month_end)


if __name__ == "__main__":
    # trade_servers_list = server_constant.get_all_trade_servers()
    db_backup_tools(('citics', 'zhongtai'))
