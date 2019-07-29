# -*- coding: utf-8 -*-
# OMAProxy重启
from eod_aps.tools.server_manage_tools import *


def oma_proxy_restart(server_name):
    server_model = server_constant.get_server_model(server_name)
    for service_name in ['OMAProxy', 'OMAServer', 'MainFrame']:
        stop_server_service(server_name, service_name)

    mainframe_flag = start_server_mainframe(server_model)

    if mainframe_flag:
        start_server_service(server_name, 'OMAServer')
        start_service_omaproxy(server_name)

        server_service_rum_cmd(server_name, 'OMAProxy', 'pull')
        time.sleep(5)
        for service_name in ['OMAServer', 'MainFrame']:
            stop_server_service(server_name, service_name)

if __name__ == '__main__':
    oma_proxy_restart('guoxin')