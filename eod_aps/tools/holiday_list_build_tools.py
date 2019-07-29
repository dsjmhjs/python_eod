# -*- coding: utf-8 -*-
# 计算非交易日期并存储入库
from eod_aps.tools.date_utils import DateUtils
from eod_aps.model.server_constans import server_constant

date_utils = DateUtils()
# servers_list = server_constant.get_all_servers()
servers_list = ['host']


holiday_dict = {
    '2013': [('2013-01-01', '2013-01-03'), ('2013-02-09', '2013-02-15'), ('2013-04-04', '2013-04-06'),
            ('2013-04-29', '2013-05-01'), ('2013-06-10', '2013-06-12'), ('2013-09-19', '2013-09-22'),
            ('2013-10-01', '2013-10-07')],
    '2014': [('2014-01-01', '2014-01-01'), ('2014-01-31', '2014-02-06'), ('2014-04-05', '2014-04-07'),
             ('2014-05-01', '2014-05-04'), ('2014-06-02', '2014-06-02'), ('2014-09-08', '2014-09-08'),
             ('2014-10-01', '2014-10-07')],
    '2015': [('2015-01-01', '2015-01-03'), ('2015-02-18', '2015-02-24'), ('2015-04-04', '2015-04-06'),
             ('2015-05-01', '2015-05-03'), ('2015-06-20', '2015-06-22'), ('2015-09-03', '2015-09-05'),
             ('2015-09-26', '2015-09-27'), ('2015-10-01', '2015-10-07')],
    '2016': [('2016-01-01', '2016-01-03'), ('2016-02-07', '2016-02-13'), ('2016-04-02', '2016-04-04'),
             ('2016-04-30', '2016-05-02'), ('2016-06-09', '2016-06-11'), ('2016-09-15', '2016-09-17'),
             ('2016-10-01', '2016-10-07')],
    '2017': [('2017-01-01', '2017-01-02'), ('2017-01-27', '2017-02-02'), ('2017-04-02', '2017-04-04'),
             ('2017-04-29', '2017-05-01'), ('2017-05-28', '2017-05-30'), ('2017-10-01', '2017-10-08'),
             ('2017-12-30', '2017-12-31')],
    '2018': [('2018-01-01', '2018-01-01'), ('2018-02-15', '2018-02-21'), ('2018-04-05', '2018-04-07'),
             ('2018-04-29', '2018-05-01'), ('2018-06-16', '2018-06-18'), ('2018-09-22', '2018-09-24'),
             ('2018-10-01', '2018-10-07')],
    '2019': [('2019-01-01', '2019-01-01'), ('2019-02-04', '2019-02-10'), ('2019-04-05', '2019-04-07'),
             ('2019-04-29', '2019-05-01'), ('2019-06-07', '2019-06-09'), ('2019-09-13', '2019-09-15'),
             ('2019-10-01', '2019-10-07')]
}

databaseList = ('172.16.10.126',)
base_weight = 0.5
holiday_weekend_dict = dict()
insert_param_list = []


def __find_weekend(year_str):
    start_date = date_utils.string_toDatetime('%s-01-01' % year_str)
    end_date = date_utils.string_toDatetime('%s-12-31' % year_str)

    for temp_day in date_utils.get_between_day_list(start_date, end_date):
        weekday_num = temp_day.weekday()
        if (weekday_num == 5) or (weekday_num == 6):
            temp_day_str = date_utils.datetime_toString(temp_day)
            if temp_day_str in holiday_weekend_dict:
                continue
            holiday_weekend_dict[temp_day_str] = (base_weight, 0)


def __find_holiday(year_str):
    holiday_list = holiday_dict[year_str]
    for (start_date_str, end_date_str) in holiday_list:
        start_date = date_utils.string_toDatetime(start_date_str)
        end_date = date_utils.string_toDatetime(end_date_str)
        for date_str in date_utils.get_between_day_list(start_date, end_date):
            holiday_date = date_utils.datetime_toString(date_str)
            holiday_weekend_dict[holiday_date] = (base_weight, 1)


def save_holiday_list(year_str):
    for server_name in servers_list:
        print server_name
        server_model = server_constant.get_server_model(server_name)
        session_history = server_model.get_db_session('history')
        del_sql = "delete from history.holiday_list where holiday >= '%s-01-01' and holiday <= '%s-12-31'" % (year_str, year_str)
        session_history.execute(del_sql)

        for (holiday_str, dict_value) in holiday_weekend_dict.items():
            (weight, is_holiday) = dict_value
            insert_sql = "insert into history.holiday_list(holiday, weight, is_holiday) values ('%s', '%s', %s)" % (holiday_str, weight, is_holiday)
            session_history.execute(insert_sql)
        session_history.commit()


def sorted_dict_values(adict):
    keys = adict.keys()
    keys.sort()
    return [dict[key] for key in keys]


def build_holiday_list(year_str):
    __find_holiday(year_str)
    __find_weekend(year_str)

    save_holiday_list(year_str)


if __name__ == '__main__':
    build_holiday_list('2019')
