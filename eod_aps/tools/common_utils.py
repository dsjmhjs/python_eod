# -*- coding: utf-8 -*-
import bcl_pb2
import datetime
from eod_aps.model.eod_const import const
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.server_constans import server_constant

date_utils = DateUtils()


class CommonUtils(object):
    """
        常用工具类
    """
    def __init__(self):
        pass

    def format_msg_time(self, input_value):
        Jan1st1970 = date_utils.string_toDatetime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        value = input_value.value
        if input_value.scale == bcl_pb2.DateTime().TICKS:
            return Jan1st1970 + datetime.timedelta(microseconds=value / 10)
        elif input_value.scale == bcl_pb2.DateTime().MILLISECONDS:
            return Jan1st1970 + datetime.timedelta(milliseconds=value)
        elif input_value.scale == bcl_pb2.DateTime().SECONDS:
            return Jan1st1970 + datetime.timedelta(seconds=value)
        elif input_value.scale == bcl_pb2.DateTime().MINUTES:
            return Jan1st1970 + datetime.timedelta(minutes=value)
        elif input_value.scale == bcl_pb2.DateTime().HOURS:
            return Jan1st1970 + datetime.timedelta(hours=value)
        elif input_value.scale == bcl_pb2.DateTime().DAYS:
            return Jan1st1970 + datetime.timedelta(days=value)
        return Jan1st1970

    def get_server_name(self, server_ip_str):
        server_name_result = ''
        for (server_name, server_model) in const.SERVER_DICT.items():
            if server_model.ip in server_ip_str:
                server_name_result = server_name
        return server_name_result
