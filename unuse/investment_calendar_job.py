# -*- coding: utf-8 -*-
# 交易日历数据管理
import sys
import urllib2
import json
import traceback
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
# 从同花顺网站获取投资日历数据
from eod_aps.model.investment_calendar import InvestmentCalendar
from eod_aps.job import *


def get_calendar_tonghua(date_str):
    base_url = 'http://comment.10jqka.com.cn/tzrl/getTzrlData.php?callback=callback_dt&type=data&date=' + date_str
    page = urllib2.urlopen(base_url)
    base_soup = BeautifulSoup(page, from_encoding='gbk')
    web_text = base_soup.text
    investment_calendar_dict = json.loads(web_text[12:len(web_text) - 2])

    investment_calendar_list = []
    for investment_calendar_item in investment_calendar_dict['data']:
        events_list = investment_calendar_item['events']
        concept_list = investment_calendar_item['concept']
        if 'field' in investment_calendar_item:
            field_list = investment_calendar_item['field']
        else:
            field_list = None

        stocks_list = investment_calendar_item['stocks']
        for i in range(0, len(events_list)):
            concept_message = []
            for concept_dict in concept_list[i]:
                concept_message.append(concept_dict['code'] + ',' + concept_dict['name'])

            field_message = []
            if field_list is not None:
                for field_dict in field_list[i]:
                    field_message.append(field_dict['code'] + ',' + field_dict['name'])

            stocks_message = []
            for stocks_dict in stocks_list[i]:
                stocks_message.append(stocks_dict['code'] + ',' + stocks_dict['name'])

            investment_calendar = InvestmentCalendar()
            investment_calendar.date = investment_calendar_item['date']
            investment_calendar.event = events_list[i][0].replace(u'•','')
            if len(concept_message) == 0:
                investment_calendar.plate = '|'.join(field_message)
            else:
                investment_calendar.plate = '|'.join(concept_message)
            investment_calendar.stocks = '|'.join(stocks_message)
            investment_calendar_list.append(investment_calendar)
    return investment_calendar_list


def navigate(base_url):
    try_times = 0
    while True:
        if try_times > 4:
            task_logger.info('Connection failed!break')
            sys.exit(-1)
        try:
            req = BeautifulSoup(urllib2.urlopen(base_url).read(), from_encoding='gbk')
            return req
        except Exception:
            error_msg = traceback.format_exc()
            try_times += 1
            task_logger.info('Connection failed!tryTimes:%s, error_msg:%s' % (str(try_times), error_msg))


def investment_calendar_job():
    server_host = server_constant.get_server_model('host')
    session_factor = server_host.get_db_session('factor')

    begin = date_utils.string_toDatetime('2014-10-01')
    end = date_utils.string_toDatetime('2016-06-01')
    d = begin

    while d < end:
        d += relativedelta(months=1)
        investment_calendar_list = get_calendar_tonghua(d.strftime("%Y%m"))
        for investment_calendar in investment_calendar_list:
            try:
                session_factor.merge(investment_calendar)
                session_factor.commit()
            except Exception:
                error_msg = traceback.format_exc()
                task_logger.error(error_msg)
                continue


    # investment_calendar_list = get_calendar_tonghua('201605')
    # for investment_calendar in investment_calendar_list:
    #     session_factor.add(investment_calendar)
    #     session_factor.commit()
    server_host.close()


if __name__ == '__main__':
    investment_calendar_job()
