# -*- coding: utf-8 -*-
import datetime
import inspect
import os
import time
import re
from dateutil.parser import parse
from eod_aps.model.eod_const import const
from cfg import custom_log


class DateUtils(object):
    """
        日期工具类
    """

    def __init__(self):
        self.__base_holiday_list = const.EOD_CONFIG_DICT['holiday_list']

    # 获取当前日期
    @staticmethod
    def get_today():
        return datetime.datetime.today().date()

    # 获取当前日期
    @staticmethod
    def get_now():
        return datetime.datetime.now()

    # 获取当前日期
    @staticmethod
    def get_today_str(format_str='%Y%m%d'):
        return datetime.datetime.now().strftime(format_str)

    # 是否是交易日
    def is_trading_day(self, date_str=None):
        if date_str is None:
            date_str = datetime.datetime.now().strftime('%Y-%m-%d')

        holiday_list = self.get_holiday_list()
        if date_str in holiday_list:
            return False
        return True

    # 是否交易时间
    def is_trading_time(self, notify_flag=True):
        now_time = long(datetime.datetime.now().strftime('%H%M%S'))
        if 0 < now_time < 23000 or 85500 < now_time < 113000 or 130000 < now_time < 150000 \
                or 205500 < now_time < 240000:
            return True
        else:
            if notify_flag:
                # 用于定位调用问题
                stack = inspect.stack()
                file_name = os.path.basename(stack[1][0].f_code.co_filename)
                line_num = stack[1][0].f_lineno
                custom_log.log_error_cmd('[%s:%s]Now Is Not Trading Time!' % (file_name, line_num))
            return False

    def is_day_market(self):
        """
            是否日盘交易时间
        """
        now_time = long(datetime.datetime.now().strftime('%H%M%S'))
        if 85500 < now_time < 113500 or 130000 < now_time < 150500:
            return True
        else:
            return False

    def is_night_market(self):
        """
            是否夜盘交易时间
        """
        now_time = long(datetime.datetime.now().strftime('%H%M%S'))
        if 0 < now_time < 23000 or 205500 < now_time < 240000:
            return True
        else:
            return False

    # 是否日盘开盘前
    def is_pre_day_market(self):
        now_time = long(datetime.datetime.now().strftime('%H%M%S'))
        if 80000 < now_time < 90000:
            return True
        else:
            return False

    # 是否夜盘开盘前
    def is_pre_night_market(self):
        now_time = long(datetime.datetime.now().strftime('%H%M%S'))
        if 200000 < now_time < 210000:
            return True
        else:
            return False

    def get_start_end_date(self):
        today_date_str = self.get_today_str('%Y-%m-%d')
        last_trading_day = self.get_last_trading_day('%Y-%m-%d')
        next_trading_day = self.get_next_trading_day()

        now_time = long(self.get_today_str('%H%M%S'))
        if 0 < now_time < 31500:
            start_date = last_trading_day + ' 21:00:00'
            end_date = today_date_str + ' 15:00:00'
        elif 81500 < now_time < 160000:
            start_date = last_trading_day + ' 21:00:00'
            end_date = today_date_str + ' 15:00:00'
        elif 201500 < now_time < 240000:
            start_date = today_date_str + ' 21:00:00'
            end_date = next_trading_day + ' 15:00:00'
        else:
            raise Exception(u"执行时间异常")
        return start_date, end_date

    # 获取前几天
    def get_last_day(self, last_num, start_date=None, format_str='%Y%m%d'):
        if start_date is None:
            start_date = datetime.datetime.now()
        last_day = start_date + datetime.timedelta(days=last_num)
        return last_day.strftime(format_str)

    def get_last_date(self, last_num, start_date=None):
        if start_date is None:
            start_date = datetime.datetime.now()
        last_date = start_date + datetime.timedelta(days=last_num)
        return last_date

    def get_last_minutes(self, last_num, start_date=None):
        if start_date is None:
            start_date = datetime.datetime.now()
        last_date = start_date + datetime.timedelta(minutes=last_num)
        return last_date

    def get_last_seconds(self, last_num, start_date=None):
        if start_date is None:
            start_date = datetime.datetime.now()
        last_date = start_date + datetime.timedelta(seconds=last_num)
        return last_date

    # 获取节假日列表
    def get_holiday_list(self, format_str='%Y-%m-%d'):
        holiday_list = []
        for holiday_str in self.__base_holiday_list:
            holiday_date = self.string_toDatetime(holiday_str)
            holiday_format_str = self.datetime_toString(holiday_date, format_str)
            holiday_list.append(holiday_format_str)
        return holiday_list

    # 获取前一交易日
    def get_last_trading_day(self, format_str, date_str=None):
        if date_str is None:
            start_date = datetime.datetime.now()
        else:
            start_date = datetime.datetime.strptime(date_str, format_str)
        last_day = start_date + datetime.timedelta(days=-1)

        holiday_list = self.get_holiday_list(format_str)
        while last_day.strftime(format_str) in holiday_list:
            last_day = last_day + datetime.timedelta(days=-1)
        return last_day.strftime(format_str)

    # 获取后一交易日
    def get_next_trading_day(self, format_str='%Y-%m-%d', date_str=None):
        if date_str is None:
            start_date = datetime.datetime.now()
        else:
            start_date = datetime.datetime.strptime(date_str, format_str)

        next_day = start_date + datetime.timedelta(days=1)

        holiday_list = self.get_holiday_list(format_str)
        while next_day.strftime(format_str) in holiday_list:
            next_day = next_day + datetime.timedelta(days=1)
        return next_day.strftime(format_str)

    # 把datetime转成字符串
    @staticmethod
    def datetime_toString(dt, format_str='%Y-%m-%d'):
        return dt.strftime(format_str)

    # 把字符串转成datetime
    @staticmethod
    def string_toDatetime(string, format_str='%Y-%m-%d'):
        return datetime.datetime.strptime(string, format_str)

    # 把字符串转成datetime
    @staticmethod
    def string_toDatetime2(string):
        return parse(string)

    # 把时间戳转成字符串形式
    @staticmethod
    def timestamp_tostring(stamp, format_str='%Y-%m-%d'):
        return time.strftime(format_str, time.localtime(stamp))

    # 把datetime类型转外时间戳形式
    @staticmethod
    def datetime_toTimestamp(dateTim):
        return time.mktime(dateTim.timetuple())

    # 把字符串转成时间戳形式
    @staticmethod
    def string_toTimestamp(strTime):
        return time.mktime(DateUtils.string_toDatetime(strTime).timetuple())

    @staticmethod
    def get_microsecond_number(time_str):
        return ((int(time_str[11:13]) * 60 + int(time_str[14:16])) * 60 + int(time_str[17:19])) * 1000000 + int(
            time_str[20:26])

    @staticmethod
    def get_seconds_number(time_str):
        return ((int(time_str[:10].replace('-', '')) * 100 + int(time_str[11:13])) * 60 + int(
            time_str[14:16])) * 60 + int(time_str[17:19])

    @staticmethod
    def count_time_number(time_str):
        return int(time_str.split(':')[0]) * 60 + int(time_str.split(':')[1])

    @staticmethod
    def get_interval_seconds(start_time_str, end_time_str):
        d1 = datetime.datetime.strptime(end_time_str[:19], "%Y-%m-%d %H:%M:%S")
        d2 = datetime.datetime.strptime(start_time_str[:19], "%Y-%m-%d %H:%M:%S")
        if d1 >= d2:
            interval_seconds = (d1 - d2).seconds
        else:
            interval_seconds = (d2 - d1).seconds
        return interval_seconds

    @staticmethod
    def get_interval_days(start_time_str, end_time_str):
        d1 = datetime.datetime.strptime(end_time_str[:10], "%Y-%m-%d")
        d2 = datetime.datetime.strptime(start_time_str[:10], "%Y-%m-%d")
        if d1 >= d2:
            interval_days = (d1 - d2).days
        else:
            interval_days = (d2 - d1).days
        return interval_days

    def get_trading_day_list(self, start_date, end_date):
        trading_day_list = []
        for i in range((end_date - start_date).days + 1):
            temp_day = start_date + datetime.timedelta(days=i)
            if self.is_trading_day(temp_day.strftime("%Y-%m-%d")):
                trading_day_list.append(temp_day)
        return trading_day_list

    def get_interval_trading_day(self, interval_num, date_str=None, format_str='%Y-%m-%d'):
        """
           查询间隔的交易日
        """
        if date_str is None:
            __interval_day = datetime.datetime.now()
        else:
            __interval_day = datetime.datetime.strptime(date_str, format_str)

        __date_size = abs(interval_num)
        while __date_size > 0:
            if interval_num > 0:
                __interval_day = __interval_day + datetime.timedelta(days=1)
            else:
                __interval_day = __interval_day + datetime.timedelta(days=-1)
            if self.is_trading_day(__interval_day.strftime("%Y-%m-%d")):
                __date_size -= 1
        return __interval_day.strftime(format_str)

    # 查询间隔多个交易日
    def get_interval_trading_day_list(self, start_date, interval_num, format_str='%Y%m%d'):
        trading_day_list = [start_date.strftime(format_str)]
        next_day = start_date
        while len(trading_day_list) < abs(interval_num):
            if interval_num > 0:
                next_day = next_day + datetime.timedelta(days=1)
            else:
                next_day = next_day + datetime.timedelta(days=-1)
            if self.is_trading_day(next_day.strftime("%Y-%m-%d")):
                trading_day_list.append(next_day.strftime(format_str))
        return trading_day_list

    def get_between_day_list(self, start_date, end_date):
        trading_day_list = []
        for i in range((end_date - start_date).days + 1):
            temp_day = start_date + datetime.timedelta(days=i)
            trading_day_list.append(temp_day)
        return trading_day_list

    def get_between_second_list(self, start_date, end_date):
        second_list = []
        for i in range((end_date - start_date).seconds + 1):
            temp_day = start_date + datetime.timedelta(seconds=i)
            second_list.append(temp_day)
        return second_list

    def get_trading_second_list(self, start_date, end_date):
        trading_second_list = []
        for i in range((end_date - start_date).seconds + 1):
            temp_day = start_date + datetime.timedelta(seconds=i)
            if '11:30:00' < self.datetime_toString(temp_day, "%H:%M:%S") < '13:00:00':
                continue
            trading_second_list.append(temp_day)
        return trading_second_list

    def get_match_date_str(self, line_str):
        reg = re.compile(r'^(0?[0-9]|1[0-9]|2[0-3]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])$')
        regMatch = reg.match(line_str)
        if regMatch:
            line_dict = regMatch.groupdict()
            print line_dict

    def compare_date_str(self, date_str1, date_str2, format_str='%Y-%m-%d'):
        date1 = self.string_toDatetime(date_str1, format_str)
        date2 = self.string_toDatetime(date_str2, format_str)
        if date1 > date2:
            return True
        else:
            return False
