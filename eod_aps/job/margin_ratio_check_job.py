# -*- coding: cp936 -*-
import sys
import json
import urllib
import urllib2
import datetime
import re
from eod_aps.job import *


reload(sys)
sys.setdefaultencoding('utf-8')


exchange_name_list = [unicode('上海', 'gbk'), unicode('大连', 'gbk'), unicode('郑州', 'gbk'), unicode('中金所', 'gbk')]
future_name_list = ['a', 'ag', 'al', 'au', 'b', 'bb', 'bu', 'c', 'cf', 'cs', 'cu', 'cy', 'fb', 'fg', 'fu', 'hc', 'i',
                    'j', 'jd', 'jm', 'jr', 'l', 'lr', 'm', 'ma', 'ni', 'oi', 'p', 'pb', 'pm', 'pp', 'rb', 'ri', 'rm',
                    'rs', 'ru', 'sf', 'ic', 'if', 'sm', 'sn', 'sr', 'ih', 't', 'ta', 'tc', 'tf', 'ts', 'v',
                    'wh', 'wr', 'y', 'zc', 'zn']

exchange_id_dict = dict()
exchange_id_dict['shfe'] = 20
exchange_id_dict['dce'] = 21
exchange_id_dict['zce'] = 22
exchange_id_dict['cff'] = 25


class future_margin_ratio_class(object):
    def __init__(self):
        self.future_name = ''
        self.future_margin_ratio = 0
        self.special_ticker_ratio_dict = dict()


def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read()
    return html


def get_margin_ratio_dict_guoxin():
    datetime_check = datetime.datetime.now()
    while True:
        datetime_str = datetime_check.strftime('%Y%m%d')
        html = getHtml("http://www.guosenqh.com.cn/main/a/%s/30815.shtml" % datetime_str)
        # with open('test.html', 'w') as f:
        #     f.write(html)
        if '404.png' in html:
            datetime_check = datetime_check - datetime.timedelta(days=1)
        else:
            break

    html = html.replace(unicode('：', 'gbk'), ':').replace(unicode('；', 'gbk'), ';').replace(unicode('％', 'gbk'), '%')

    table_flag = False
    start_flag = False
    future_margin_ratio_info_list = []
    for line in html.split('\n'):
        if unicode('公司各品种现行保证金标准', 'gbk') in line:
            date_str_temp = line.split(unicode('（', 'gbk'))[1].split(unicode('）', 'gbk'))[0]
            date_str = date_str_temp.split(unicode('日', 'gbk'))[0].replace(unicode('年', 'gbk'), '-'). \
                replace(unicode('月', 'gbk'), '-')
            [year, month, day] = date_str.split('-')
            if len(month) == 1:
                month = '0' + month
            if len(day) == 1:
                day = '0' + day
            date_str = year + '-' + month + '-' + day

        if '<TABLE' in line or '<table' in line:
            table_flag = True
        if not table_flag:
            continue
        if '<TR' in line or '<tr' in line:
            start_flag = True
            future_margin_ratio_info_temp = []
        if '</TR>' in line or '</tr' in line:
            if len(future_margin_ratio_info_temp) > 1:
                if future_margin_ratio_info_temp[1].lower() in future_name_list:
                    future_margin_ratio_info_list.append(future_margin_ratio_info_temp)
            start_flag = False

        if start_flag:
            if '</TD>' in line:
                table_info = line.split('</TD>')[-2].split('>')[-1]
                if table_info not in exchange_name_list:
                    future_margin_ratio_info_temp.append(table_info)
            if '</td>' in line:
                table_info = line.split('</td>')[-2].split('>')[-1]
                if table_info not in exchange_name_list:
                    future_margin_ratio_info_temp.append(table_info)

    future_margin_ratio_info_class_dict = dict()
    for margin_info in future_margin_ratio_info_list:
        future_margin_ratio_info_class = future_margin_ratio_class()
        future_margin_ratio_info_class.future_name = margin_info[1]
        future_margin_ratio_info_class.future_margin_ratio = float(margin_info[2].replace('%', '')) / 100
        for ticker_ratio in margin_info[3].replace(';', ',').replace('&nbsp;', '').split(unicode(',', 'gbk')):
            if ':' in ticker_ratio:
                for ticker_info in re.findall(r'\w+[\\?\w+]*:\d+%', ticker_ratio):
                    # print ticker_info
                    ticker_name = ticker_info.split(':')[0]
                    # print ticker_info.split(':')[1].split('%')[0]
                    ticker_margin_ratio = float(ticker_info.split(':')[1].split('%')[0]) / 100
                    future_margin_ratio_info_class.special_ticker_ratio_dict[ticker_name] = ticker_margin_ratio
        margin_info_4_temp = margin_info[4].replace('&nbsp;', '').replace(unicode('；', 'gbk'), '|'). \
            replace(unicode('，', 'gbk'), '|')
        for ticker_ratio in re.findall(r'\w+[\\?\w+]*:\d+%', margin_info_4_temp):
            if ':' in ticker_ratio:
                ticker_name = ticker_ratio.split(':')[0]
                ticker_margin_ratio = float(ticker_ratio.split(':')[1].replace('%', '')) / 100
                future_margin_ratio_info_class.special_ticker_ratio_dict[ticker_name] = ticker_margin_ratio
            future_margin_ratio_info_class_dict[margin_info[1]] = future_margin_ratio_info_class

    return future_margin_ratio_info_class_dict, date_str


def get_margin_ratio_dict_shfe():
    datetime_check = datetime.datetime.now()
    while True:
        datetime_str = datetime_check.strftime('%Y%m%d')
        url = "http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument%s.dat" % datetime_str
        html = getHtml(url)
        if '<title>404</title>' in html:
            datetime_check = datetime_check - datetime.timedelta(days=1)
        else:
            break

    margin_ratio_info_dict = json.loads(html)
    margin_ratio_list = margin_ratio_info_dict['ContractDailyTradeArgument']
    first_ticker_flag = True
    margin_ratio_dict = dict()
    for margin_ratio_info in margin_ratio_list:
        if first_ticker_flag:
            date_str = margin_ratio_info['UPDATE_DATE'].split(' ')[0]
            first_ticker_flag = False
        margin_ratio_dict[margin_ratio_info['INSTRUMENTID']] = float(margin_ratio_info['SPEC_LONGMARGINRATIO'])
    return margin_ratio_dict, date_str


def get_margin_ratio_dict_dce():
    url = "http://www.dce.com.cn/publicweb/notificationtips/queryDayTradPara.html"
    html = getHtml(url)

    start_flag = False
    margin_ratio_value_dict = dict()
    for line in html.split('\n'):
        if unicode('大连商品交易所', 'gbk') in line and unicode('日合约参数一览表', 'gbk') in line:
            date_str = line.strip().split(unicode('大连商品交易所', 'gbk'))[1].split(unicode('日合约参数一览表', 'gbk'))[0].replace(
                ' &nbsp;', '')
            date_str = date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:]

        if '<tr><td>' in line and '-' not in line:
            start_flag = True
            ticker_name = line.strip().split('<tr><td>')[1].split('</td><td>')[0]

        if start_flag:
            number_flag = True
            if line.strip() == '':
                continue
            for i in line.strip():
                if i == '.':
                    continue
                if i.isalpha():
                    number_flag = False
            if number_flag:
                margin_ratio_value = float(line.strip())
                margin_ratio_value_dict[ticker_name] = margin_ratio_value
                start_flag = False
    return margin_ratio_value_dict, date_str


def get_margin_ratio_dict_zce():
    datetime_check = datetime.datetime.now()
    for i in range(7):
        datetime_str = datetime_check.strftime('%Y%m%d')
        year_str = datetime_check.strftime('%Y')
        real_url = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataClearParams.txt" \
                   % (year_str, datetime_str)
        headers = {
            'Host': 'www.czce.com.cn',  # domain and others header
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/16.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive'
        }
        req = urllib2.Request(real_url, headers=headers)
        try:
            res_data = urllib2.urlopen(req)
            res = res_data.read()
            break
        except:
            datetime_check = datetime_check - datetime.timedelta(days=1)

    margin_ratio_dict = dict()
    for line in res.split('\n'):
        if line.strip() == '':
            continue
        if '郑州商品交易所期货结算参数表' in line:
            date_str = unicode(line, 'gbk').split('(')[1].split(')')[0]
            continue
        if '合约代码' in line:
            continue
        line_new = line.replace(' ', '')
        ticker_name = line_new.split('|')[0]
        margin_ratio_value = float(line_new.split('|')[4]) / 100
        margin_ratio_dict[ticker_name] = margin_ratio_value
    return margin_ratio_dict, date_str


def get_margin_ratio_dict_cff():
    target_time = datetime.datetime.now()
    while True:
        date_str = target_time.strftime('%Y%m%d')
        year_mon_str = target_time.strftime('%Y%m')
        day_str = target_time.strftime('%d')
        url = "http://www.cffex.com.cn/sj/jscs/%s/%s/%s_1.csv" % (year_mon_str, day_str, date_str)
        html = getHtml(url)
        if 'IC' in html and 'IH' in html and 'IF' in html:
            break
        else:
            target_time = target_time - datetime.timedelta(days=1)

    margin_ratio_dict = dict()
    for line in html.split('\n'):
        if line.strip() == '':
            continue
        if '期货合约结算业务参数表' in line:
            date_str = line.split('（')[1].split('）')[0]
            continue
        if '合约多头保证金标准' in line:
            continue
        line_new = line.replace(' ', '')
        ticker_name = line_new.split(',')[0]
        margin_ratio_value = float(line_new.split(',')[1].replace('%', '')) / 100
        margin_ratio_dict[ticker_name] = margin_ratio_value
    date_str = date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:]
    return margin_ratio_dict, date_str


def build_html_send_email(exchange_update_time_dict, margin_ratio_info_list):
    email_content_list = []
    table_list = '<table border="1">'
    table_header = '<tr><th align="center" font-size:12px; bgcolor="#70bbd9"><b>Exchange Name</b></th>'
    table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>Update Time</b></th></tr>'
    table_list += table_header
    # exchange_name_list = ['guoxin', 'shfe', 'dce', 'zce', 'cff']
    exchange_name_list = ['guoxin', 'shfe', 'dce', 'cff']
    for exchange_name in exchange_name_list:
        table_line = '<tr><td align="center" font-size:12px; bgcolor="#ee4c50"><b>%s</b></td>' % exchange_name
        table_line += '<td align="center" font-size:12px;><b>%s</b></td></tr>' % exchange_update_time_dict[
            exchange_name]
        table_list += table_line

    table_list += '</table>'
    email_content_list.append(table_list)

    table_list = '<table border="1">'
    table_header = '<tr><th align="center" font-size:12px; bgcolor="#70bbd9"><b>Ticker Name</b></th>'
    table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>Local</b></th>'
    table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>Exchange</b></th>'
    table_header += '<th align="center" font-size:12px; bgcolor="#70bbd9"><b>Guoxin</b></th>'
    table_header += '</tr>'
    table_list += table_header

    for margin_ratio_info in margin_ratio_info_list:
        table_line = '<tr><td align="center" font-size:12px; bgcolor="#ee4c50"><b>%s</b></td>' % margin_ratio_info[0]
        table_line += '<th align="center" font-size:12px;><b>%s</b></th>' % margin_ratio_info[1]
        if margin_ratio_info[2] == 'Null':
            table_line += '<th align="center" font-size:12px; bgcolor="#ee4c50"><b>%s</b></th>' % margin_ratio_info[2]
        elif float(margin_ratio_info[2]) != float(margin_ratio_info[1]):
            table_line += '<th align="center" font-size:12px; bgcolor="#ee4c50"><b>%s</b></th>' % margin_ratio_info[2]
        else:
            table_line += '<th align="center" font-size:12px;><b>%s</b></th>' % margin_ratio_info[2]
        table_line += '<th align="center" font-size:12px;><b>%s</b></th>' % margin_ratio_info[3]
        table_line += '</tr>'
        table_list += table_line
    table_list += '</table>'
    email_content_list.append(table_list)
    email_utils2.send_email_group_all(unicode('保证金比例对比', 'gbk'), '<br><br><br>'.join(email_content_list), 'html')


def get_ticker_future_name(ticker_name):
    future_name = ''
    for i in ticker_name:
        if i.isalpha():
            future_name += i
    return future_name


def margin_ratio_check_job():
    margin_ratio_info_dict = dict()
    margin_ratio_info_dict['guoxin'] = get_margin_ratio_dict_guoxin()
    margin_ratio_info_dict['shfe'] = get_margin_ratio_dict_shfe()
    margin_ratio_info_dict['dce'] = get_margin_ratio_dict_dce()
    margin_ratio_info_dict['zce'] = get_margin_ratio_dict_zce()
    margin_ratio_info_dict['cff'] = get_margin_ratio_dict_cff()

    server_model_host = server_constant.get_server_model('host')
    session_common = server_model_host.get_db_session('common')
    exchange_update_time_dict = dict()
    exchange_update_time_dict['guoxin'] = margin_ratio_info_dict['guoxin'][1]
    margin_ratio_info_list = []
    for [exchange_name, exchange_id] in sorted(exchange_id_dict.items()):
        if exchange_name == 'zce':
            continue
        date_str = margin_ratio_info_dict[exchange_name][1]
        exchange_update_time_dict[exchange_name] = date_str
        query_sql = "select ticker, longmarginratio from common.instrument where exchange_id = %s and DEL_FLAG = 0" \
                    " and type_id = 1 order by ticker asc;" % exchange_id
        query_result = session_common.execute(query_sql)
        for query_line in query_result:

            if query_line[0] in margin_ratio_info_dict[exchange_name][0]:
                exchange_margin_ratio = margin_ratio_info_dict[exchange_name][0][query_line[0]]
            else:
                exchange_margin_ratio = 'Null'

            future_name = get_ticker_future_name(query_line[0])
            if future_name not in margin_ratio_info_dict['guoxin'][0]:
                continue
            margin_ratio_info = margin_ratio_info_dict['guoxin'][0][future_name]
            if future_name in margin_ratio_info_dict['guoxin'][0]:
                if query_line[0] not in margin_ratio_info.special_ticker_ratio_dict:
                    exchange_margin_ratio_guoxin = margin_ratio_info.future_margin_ratio
                else:
                    exchange_margin_ratio_guoxin = margin_ratio_info.special_ticker_ratio_dict[query_line[0]]
            else:
                exchange_margin_ratio_guoxin = 'Null'
            margin_ratio_info_list.append([query_line[0], query_line[1], exchange_margin_ratio, exchange_margin_ratio_guoxin])

    build_html_send_email(exchange_update_time_dict, margin_ratio_info_list)
    server_model_host.close()


if __name__ == '__main__':
    margin_ratio_check_job()
