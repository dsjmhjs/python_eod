#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import os
import shutil
import re
from eod_aps.job import *
from eod_aps.tools.dokcer_manager_tool import DockerManager
from eod_aps.tools.file_utils import FileUtils


class Tensorflow_init(object):

    def __init__(self, server_name):
        self.__server_name = server_name
        self.docker_obj = DockerManager(self.__server_name)

    def op_docker(self, action, container):
        self.docker_obj.manager_docker(action, container)

    def start_ServerProxy(self):
        server_model = server_constant.get_server_model(self.__server_name)
        start_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                          './script/start.serving_proxy.sh'
                          ]
        server_model.run_cmd_str(';'.join(start_cmd_list))

    def stop_ServerProxy(self):
        server_model = server_constant.get_server_model(self.__server_name)
        start_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_project_folder'],
                          'screen -r serving_proxy -X quit'
                          ]
        server_model.run_cmd_str(';'.join(start_cmd_list))

    def check_server_proxy_status(self):
        data_str = date_utils.get_today_str()
        server_model = server_constant.get_server_model(self.__server_name)
        tradeplat_log_folder = '%s/log' % server_model.server_path_dict['tradeplat_project_folder']
        log_file = server_model.run_cmd_str(
            'ls  %s | grep screenlog_serving_proxy_%s | tail -n1' % (tradeplat_log_folder, data_str))
        log_info = server_model.run_cmd_str("cd %s;tail -n100 %s" % (tradeplat_log_folder, log_file))
        rst_e = re.findall(r': E ', log_info)
        rst_f = re.findall(r': F ', log_info)
        if rst_e or rst_f:
            email_utils2.send_email_group_all('[Error]server_proxy error', log_info)

    def check_docker_status(self):
        docker_status_info = self.docker_obj.manager_docker('status', 'stkintraday_d1')
        if not docker_status_info or 'stkintraday_d1' not in docker_status_info.split():
            email_utils2.send_email_group_all('[Error]Dokcer status error', 'Dokcer stkintraday_d1 已停止运行')
            return False
        else:
            return True

    def check_tensorflow_status(self):
        if self.check_docker_status():
            docker_log_info = self.docker_obj.manager_docker('logs', 'stkintraday_d1')
            rst = re.findall(r': F ', docker_log_info)
            e_rst = re.findall(r': E ', docker_log_info)
            if rst or e_rst:
                email_utils2.send_email_group_all('[Error]Tensorflow error', docker_log_info)
                return False
            else:
                return True
        else:
            return False


if __name__ == '__main__':
    tensorflow_init = Tensorflow_init('huabao')
    tensorflow_init.check_server_proxy_status()
    # start_ServerProxy('huabao_test')
    # shutil.copytree('D:\work\CTP', 'Z:\dailyjob\daily_server_files\huabao\TradePlat\models')
