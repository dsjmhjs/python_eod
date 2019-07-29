# -*- coding: utf-8 -*-
from xmlrpclib import ServerProxy
from eod_aps.model.eod_const import const


WIND_WSDL_ADDRESS = const.EOD_CONFIG_DICT['wind_wsdl_address']

def w_ys():
    s = ServerProxy(WIND_WSDL_ADDRESS, allow_none=True)
    s.w_close()
    s.start()
    return s


def w_ys_close():
    s = ServerProxy(WIND_WSDL_ADDRESS, allow_none=True)
    s.w_close()


if __name__ == '__main__':
    w_ys()