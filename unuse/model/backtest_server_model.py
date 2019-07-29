# -*- coding: utf-8 -*-
from eod_aps.model.server_model import ServerModel


class BackTestServerModel(ServerModel):
    backtest_cpp_folder = '/home/backtest/apps/BackTest'
    backtest_config_folder = backtest_cpp_folder + '/config'
    backtest_result_folder = backtest_cpp_folder + '/result'
    userName = 'trader'
    passWord = '123@trader'

    def __init__(self, name):
        ServerModel.__init__(self, name)
