# -*- coding: utf-8 -*-
from eod_aps.tools.config_parser_local import *
from eod_aps.model.eod_const import const


class ServerConstantLocal(object):
    server_dict = dict()

    def __init__(self):
        if len(const.CONFIG_SERVER_LIST) == 0:
            get_config_server_list()
        config_server_list = const.CONFIG_SERVER_LIST

        for config_server in config_server_list:
            self.server_dict[config_server.name] = config_server

    def get_server_model(self, server_name):
        return self.server_dict[server_name]


server_constant_local = ServerConstantLocal()
