# -*- coding: utf-8 -*-
import os
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.email_utils import EmailUtils
from eod_aps.model.eod_const import const

parameter_folder_path = 'Z:/dailyjob/cta_update_info/para_insert_sql/'
email_utils = EmailUtils(const.EMAIL_DICT['group14'])

def load_server_strategy_parameter(server_name):
    server_model = server_constant.get_server_model(server_name)
    session_strategy = server_model.get_db_session('strategy')
    parameter_file_path = parameter_folder_path + '%s/para_insert_sql_file.txt' % server_name
    if not os.path.exists(parameter_file_path):
        return
    fr = open(parameter_file_path)
    for line in fr.readlines():
        if line.strip() == '':
            continue
        insert_sql = line.strip()
        session_strategy.execute(insert_sql)
    session_strategy.commit()
    server_model.close()


def load_strategy_parameter():
    try:
        for server_name in server_constant.get_cta_servers():
            load_server_strategy_parameter(server_name)
    except:
        import traceback
        email_utils.send_email_group_all('[Error]Load Strategy Parameter', traceback.format_exc(), 'html')
    return 0


if __name__ == "__main__":
    load_strategy_parameter()