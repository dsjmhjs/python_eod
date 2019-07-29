# -*- coding: utf-8 -*-
from eod_aps.model.server_constans import server_constant
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def version_check(server_name_list):
    result_list = []
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                    'ls -l build64_release'
                    ]
        result_str = server_model.run_cmd_str(';'.join(cmd_list))

        result_list.append('%s:\n%s' % (server_name, result_str))
    print '\n'.join(result_list)


if __name__ == '__main__':
    trade_servers_list = server_constant.get_trade_servers()
    version_check(trade_servers_list)