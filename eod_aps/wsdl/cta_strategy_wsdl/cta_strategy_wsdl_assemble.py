# -*- coding: utf-8 -*-
import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer
from eod_aps.wsdl.cta_strategy_wsdl.strategy_init_tools import backtest_init
from eod_aps.wsdl.cta_strategy_wsdl.load_strategy_parameter_tools import load_strategy_parameter
from eod_aps.wsdl.cta_strategy_wsdl.strategy_online_offline_tools import strategy_online_offline_job
from eod_aps.model.server_constans import server_constant


def cta_test():
    cta_test_str = ""
    cta_server_list = server_constant.get_cta_servers()
    for cta_server in cta_server_list:
        server_model = server_constant.get_server_model(cta_server)
        result_str = server_model.run_cmd_str('ls')
        if 'apps' in result_str:
            cta_test_str += '%s: connect success!\n' % cta_server
        else:
            cta_test_str += '%s: connect error!\n' % cta_server
    return cta_test_str


def insert_strategy_state_sql():
    cta_server_list = server_constant.get_cta_servers()
    from eod_aps.job.insert_strategy_state_sql_job import insert_strategy_state_sql_job
    insert_strategy_state_sql_job(cta_server_list)
    return 0


if __name__ == '__main__':
    s = SimpleXMLRPCServer((socket.gethostbyname(socket.gethostname()), 8000))
    s.register_function(cta_test)
    s.register_function(backtest_init)
    s.register_function(load_strategy_parameter)
    s.register_function(strategy_online_offline_job)
    s.register_function(insert_strategy_state_sql)
    s.serve_forever()
