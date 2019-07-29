#!/usr/bin/env python
# _*_ coding:utf-8 _*_
from eod_aps.model.server_constans import ServerConstant

server_constant = ServerConstant()


class LocalServerManager(object):
    def __init__(self, server_name):
        self.__server_name = server_name

    def barra_report(self, filter_date_str):
        server_model = server_constant.get_server_model(self.__server_name)
        server_model.run_cmd_str('/home/routine/barra_report/main.py %s' % filter_date_str)


if __name__ == '__main__':
    pass
