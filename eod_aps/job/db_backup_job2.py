# -*- coding: utf-8 -*-
# 实现数据库文件备份
import os
import pickle
import traceback
from eod_aps.model.server_constans import ServerConstant
from eod_aps.tools.date_utils import DateUtils

date_utils = DateUtils()
server_constant = ServerConstant()
dbNames = ['common', 'history', 'om', 'portfolio', 'strategy']

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


def date_backup_pickle():
    try:
        today_str = date_utils.get_today_str('%Y-%m-%d')
        last_day_str = date_utils.get_last_trading_day('%Y-%m-%d')
        server_model = server_constant.get_server_model('guoxin')
        # db_backup_folder = server_model.server_path_dict['db_backup_folder']
        db_backup_folder = 'G:/pickle'

        session_common = server_model.get_db_session('common')
        for (full_table_name, columns_infos) in backup_table_dict.items():
            schema_name, table_name = full_table_name.split('.')
            (query_columns_name, query_columns_type) = columns_infos
            query_columns_sql = "select COLUMN_NAME from information_schema.COLUMNS " \
                                "where table_schema = '%s' and table_name = '%s'" % (schema_name, table_name)

            column_list = ['`%s`' % x[0] for x in session_common.execute(query_columns_sql)]

            if query_columns_type == 'Date':
                query_data_sql = "select %s from %s.%s where %s = '%s'" % \
                                 (','.join(column_list), schema_name, table_name, query_columns_name, today_str)
            elif query_columns_type == 'Time':
                query_data_sql = "select %s from %s.%s where %s >= '%s 18:00:00'" % \
                                 (','.join(column_list), schema_name, table_name, query_columns_name, last_day_str)

            replace_sql_list = []
            for x in session_common.execute(query_data_sql):
                value_list = []
                for i in range(0, len(column_list)):
                    if x[i] is None:
                        value_list.append('Null')
                    # elif x[i].isdigit():
                    #     value_list.append(x[i])
                    else:
                        value_list.append("\'%s\'" % x[i])
                replace_sql = "REPLACE INTO %s (%s) VALUES (%s) " % (full_table_name, ','.join(column_list),
                                                                     ','.join(value_list))
                replace_sql_list.append(replace_sql)
            pickle_file_name = '%s_%s.pickle' % (table_name, today_str)
            pickle_file_path = '%s/%s' % (db_backup_folder, pickle_file_name)
            with open(pickle_file_path, 'wb') as f:
                pickle.dump(replace_sql_list, f, True)
    except Exception:
        error_msg = traceback.format_exc()
        print error_msg


def update_by_pickle_file():
    server_host = server_constant.get_server_model('host')
    session_common = server_host.get_db_session('common')
    # pickle_file_folder = '%s/%s' % (server_host.server_path_dict['tradeplat_project_folder'], 'pickle_file')
    pickle_file_folder = 'G:/pickle'
    for pickle_file_name in os.listdir(pickle_file_folder):
        pickle_file_path = '%s/%s' % (pickle_file_folder, pickle_file_name)
        with open(pickle_file_path, 'rb') as f:
            update_sql_list = pickle.load(f)
        for update_sql in update_sql_list:
            session_common.execute(update_sql)
    session_common.commit()
    session_common.close()


if __name__ == '__main__':
    # date_backup_pickle()
    update_by_pickle_file()
