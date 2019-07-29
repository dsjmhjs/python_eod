# -*- coding: utf-8 -*-
import urllib
import urllib2
import datetime
import requests
from bs4 import BeautifulSoup

# http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/rcjccpm/index.html
def query_dce1(ticker_type, date_str):
    temp_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    request_url = 'http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html'
    test_data = {'memberDealPosiQuotes.variety': ticker_type,
                 'memberDealPosiQuotes.trade_type': '0',
                 'year': temp_date.year,
                 'month':  temp_date.month - 1,
                 'day': temp_date.day,
                 'contract.contract_id': 'all',
                 'contract.variety_id': ticker_type
                 }
    test_data_urlencode = urllib.urlencode(test_data)
    req = urllib2.Request(url=request_url, data=test_data_urlencode)
    res_data = urllib2.urlopen(req)
    base_soup = BeautifulSoup(res_data.read(), 'lxml')
    table_items = base_soup.find_all('table')
    for table_item in table_items:
        title_list = []
        table_list = []
        for tr_item in table_item.find_all('tr'):
            if tr_item.find_all('th'):
                for th_item in tr_item.find_all('th'):
                    title_list.append(th_item.text)

            if tr_item.find_all('td'):
                tr_list = []
                for th_item in tr_item.find_all('td'):
                    tr_list.append(th_item.text)
                table_list.append(tr_list)
        print title_list, table_list


# http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/cdrb/index.html
def query_dce2(date_str):
    temp_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    request_url = 'http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html'
    test_data = {'wbillWeeklyQuotes.variety: ': 'all',
                 'year': temp_date.year,
                 'month':  temp_date.month - 1,
                 'day': temp_date.day
                 }
    test_data_urlencode = urllib.urlencode(test_data)
    req = urllib2.Request(url=request_url, data=test_data_urlencode)
    res_data = urllib2.urlopen(req)
    base_soup = BeautifulSoup(res_data.read(), 'lxml')
    table_items = base_soup.find_all('table')
    for table_item in table_items:
        title_list = []
        table_list = []
        for tr_item in table_item.find_all('tr'):
            if tr_item.find_all('th'):
                for th_item in tr_item.find_all('th'):
                    title_list.append(th_item.text)

            if tr_item.find_all('td'):
                tr_list = []
                for th_item in tr_item.find_all('td'):
                    tr_list.append(th_item.text)
                table_list.append(tr_list)
        print title_list, table_list


def query_czce1(date_str, url_template):
    temp_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    year_str = temp_date.year
    date_str = temp_date.strftime('%Y%m%d')

    request_url = url_template % (year_str, date_str)
    r = requests.get(request_url)
    r.encoding = r.apparent_encoding

    base_soup = BeautifulSoup(r.text, "html.parser")
    table_items = base_soup.find_all('table')
    for table_item in table_items:
        table_list = []
        for tr_item in table_item.find_all('tr'):
            tr_list = []
            for th_item in tr_item.find_all('td'):
                tr_list.append(th_item.text)
            table_list.append(tr_list)
        print table_list


if __name__ == '__main__':
    # ticker_type = 'a'
    # date_str = '2018-08-02'
    # query_dce1(date_str)

    # date_str = '2018-08-02'
    # query_dce2(date_str)

    date_str = '2018-08-02'
    # url_template1 = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataWhsheet.htm'
    # url_template2 = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm'
    url_template3 = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataTradeamt.htm'
    query_czce1(date_str, url_template3)
