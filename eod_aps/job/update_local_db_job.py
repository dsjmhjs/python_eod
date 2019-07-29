# -*- coding: utf-8 -*-
# 更新本地数据库的行情数据
import os
from eod_aps.job import *

SERVER_PYTHON_FOLDER = '%s/eod_aps/server_python' % EOD_PROJECT_FOLDER


# 新增数据至本地和服务器的数据库
def __add_by_market_file():
    add_flag = False
    os.chdir(SERVER_PYTHON_FOLDER)
    # output = os.popen('python femas_price_analysis_add.py')
    # market_add_message = output.read()
    # if 'prepare insert' in market_add_message:
    #     add_flag = True
    # custom_log.log_info_task(market_add_message)

    output = os.popen('python ctp_price_analysis_add.py')
    market_add_message = output.read()
    if 'prepare insert' in market_add_message:
        add_flag = True
    custom_log.log_info_job(market_add_message)

    output = os.popen('python Lts_price_analysis_add.py')
    market_add_message = output.read()
    if 'prepare insert' in market_add_message:
        add_flag = True
    custom_log.log_info_job(market_add_message)
    return add_flag


def __update_by_market_file():
    os.chdir(SERVER_PYTHON_FOLDER)
    # output = os.popen('python femas_price_analysis.py')
    # print output.read()

    output = os.popen('python ctp_price_analysis.py')
    custom_log.log_info_job(output.read())

    output = os.popen('python Lts_price_analysis.py')
    custom_log.log_info_job(output.read())


def __update_by_etf_file():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python update_by_etf_file.py')
    custom_log.log_info_job(output.read())


def update_local_market_job():
    __add_by_market_file()
    __update_by_market_file()


def update_local_market_job_pandas():
    os.chdir(SERVER_PYTHON_FOLDER)
    output = os.popen('python pandas_ctp_price_file.py')
    return_message = output.read()
    custom_log.log_info_job(return_message)

    output = os.popen('python pandas_lts_price_file.py')
    return_message = output.read()
    custom_log.log_info_job(return_message)


def update_local_etf_job():
    __update_by_etf_file()


if __name__ == '__main__':
    __add_by_market_file()
