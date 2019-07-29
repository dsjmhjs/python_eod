# -*- coding: utf-8 -*-
import os
import sys
from eod_aps.server_python import *


def __instrument_market_start():
    __instrument_market_stop()
    os.chdir(MKTDTCTR_PROJECT_FOLDER)
    os.popen('./script/start.capture_l2_tcp.sh')


def __instrument_market_stop():
    os.chdir(MKTDTCTR_PROJECT_FOLDER)
    os.popen('./script/stop.capture_l2_tcp.sh')


def instrument_market_manager(manager_type):
    if not date_utils.is_trading_day():
        return
    if manager_type == 'start':
        __instrument_market_start()
    elif manager_type == 'stop':
        __instrument_market_stop()


if __name__ == '__main__':
    manager_type = str(sys.argv[1]).strip()
    instrument_market_manager(manager_type)
