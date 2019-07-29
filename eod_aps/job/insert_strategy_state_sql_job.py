# -*- coding: utf-8 -*-
import os
from eod_aps.job import *


def insert_strategy_state_sql_job(server_name_list):
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        session_strategy = server_model.get_db_session('strategy')
        save_folder_name = server_name
        backtest_state_insert_folder = BACKTEST_STATE_INSERT_FOLDER + '%s/' % save_folder_name
        for file_name in os.listdir(backtest_state_insert_folder):
            insert_sql_str = ''
            with open(backtest_state_insert_folder + file_name, 'rb') as fr:
                for line in fr.readlines():
                    if line.strip() == '':
                        continue
                    insert_sql_str = line.strip()
            if insert_sql_str != '':
                session_strategy.execute(insert_sql_str)
        session_strategy.commit()
        server_model.close()


if __name__ == "__main__":
    insert_strategy_state_sql_job(['nanhua', 'zhongxin', 'luzheng'])
