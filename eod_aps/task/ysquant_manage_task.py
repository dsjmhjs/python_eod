# -*- coding: utf-8 -*-
from eod_aps.job.ysquant_manage_job import *
from eod_logger import log_wrapper, log_trading_wrapper

date_utils = DateUtils()

@log_trading_wrapper
def index_minute():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'index_minute')


@log_trading_wrapper
def minute_sharry_writer():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'minute_sharry_writer')


@log_wrapper
def daily_sharry_writer1():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'daily_sharry_writer1')


@log_trading_wrapper
def daily_sharry_writer2():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'daily_sharry_writer2')


@log_trading_wrapper
def index_bin():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'index_bin')


@log_wrapper
def not_daily_reader():
    ysquant_server_list = server_constant.get_ysquant_servers()
    ysquant_cmd_job(ysquant_server_list, 'not_daily_reader')


@log_trading_wrapper
def basic_sharry():
    ysquant_server_list = server_constant.get_ysquant_servers()
    print ysquant_server_list
    # ysquant_cmd_job(ysquant_server_list, 'basic_sharry')


if __name__ == '__main__':
    ysquant_cmd_job(('ysquant_168', ), 'not_daily_reader')
    ysquant_cmd_job(('ysquant_168', ), 'daily_sharry_writer1')
    ysquant_cmd_job(('ysquant_168', ), 'basic_sharry')
    ysquant_cmd_job(('ysquant_168', ), 'index_bin')

