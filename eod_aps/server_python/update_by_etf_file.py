# -*- coding: utf-8 -*-
# 根据文件更新pcf信息
import os
import json
import xlrd
import csv
from collections import OrderedDict
from lxml import etree
from decimal import Decimal
from StringIO import StringIO
from datetime import datetime
from eod_aps.model.schema_common import Instrument
from eod_aps.model.eod_parse_arguments import parse_arguments
from eod_aps.server_python import *


index_SHSZ300List = []
index_SSE50List = []
index_SH000905List = []
update_instrument_dict = dict()
instrument_db_dict = dict()


def __read_pcf_file(date_str):
    if date_str is None or date_str == '':
        filter_key1 = date_utils.get_today_str('%Y%m%d')
        filter_key2 = date_utils.get_today_str('%m%d')
    else:
        filter_key1 = date_str.replace('-', '')
        filter_key2 = filter_key1[4:]

    for rt, dirs, files in os.walk(ETF_FILE_PATH):
        for f in files:
            etf_file_path = rt + "/" + f
            if f[-3:] == 'xls':
                __read_pcf_file_xls(etf_file_path)
                continue
            elif ('Shanghai' in f) and (f[-3:] == 'csv'):
                if filter_key1 not in f:
                    continue
                __read_pcf_file_csv(etf_file_path)
                continue
            elif f[-3:] == 'xml':
                if not f.startswith('pcf'):
                    continue
                if filter_key1 not in f:
                    continue

                with open(etf_file_path, 'r') as input_value:
                    if 'FILEBEGIN' in input_value.read():
                        __read_pcf_file_txt(etf_file_path, str(f[4:10]))
                    else:
                        __read_pcf_file_xml(etf_file_path)
                continue
            elif f[-3:] == 'ETF':
                if filter_key2 not in f:
                    continue

                if f[:4] == '50__':
                    ticker = 510050
                elif f[:4] == '180_':
                    ticker = 510180
                elif f[:4] == 'HL__':
                    ticker = 510880
                elif f[:4] == 'YQ50':
                    ticker = 510060
                elif f[-3:] == 'ETF':
                    ticker = f[:6]
                else:
                    print 'Error File:' + f
                    continue
                __read_pcf_file_txt(etf_file_path, str(ticker))
                continue
            elif f[-3:] == 'PCF':
                __read_pcf_file_txt(etf_file_path, str(f[:6]))
                continue

    print 'SHSZ300 size:%s' % len(index_SHSZ300List)
    print 'SH000905 size:%s' % len(index_SSE50List)
    print 'SHSZ300 size:%s' % len(index_SH000905List)
    _fetch_index(index_SHSZ300List, 'SHSZ300')
    _fetch_index(index_SSE50List, 'SSE50')
    _fetch_index(index_SH000905List, 'SH000905')


def __read_pcf_file_txt(txt_file_path, ticker):
    etf_dict = OrderedDict()
    stock_list = []

    with open(txt_file_path, 'r') as input_value:
        lines = input_value.read().split('\n')

    etf_dict['Ticker'] = ticker
    for l in lines:
        if len(l) < 3:
            continue
        l = l.strip().decode('utf-8', 'ignore')
        if '=' in l:
            [key, data] = l.split('=')
            if key == 'FundID':
                key = 'Ticker'
            elif key == 'IsPublishIPOV':
                if data == '1':
                    data = 'True'
                elif data == '0':
                    data = 'False'
            elif key == 'NAV':
                nav = data
            if key == 'CreationRedemption':
                if data == '1':
                    etf_dict['Creation'] = '1'
                    etf_dict['Redemption'] = '1'
                elif data == '2':
                    etf_dict['Creation'] = '1'
                    etf_dict['Redemption'] = '0'
                else:
                    etf_dict['Creation'] = '0'
                    etf_dict['Redemption'] = '0'
                continue
            etf_dict[key] = data.replace('\r', '')
        elif '|' in l:
            if 'Type' in etf_dict:
                if etf_dict['Type'] in ['2', '4']:
                    continue
                if len(l.split('|')) == 10:
                    [head, ticker, symbol, shr, allow, prcnt, fix, fix1, mrkt, end] = l.split('|')
                elif len(l.split('|')) == 9:
                    [ticker, symbol, shr, allow, prcnt, fix, fix1, mrkt, end] = l.split('|')
                else:
                    continue
                d = dict()
                d["Ticker"] = ticker
                if shr == '        ':
                    shr = '0'
                    d['Share'] = int(shr)
                d['Share'] = int(shr)

                if allow == '0':
                    allow = 'Forbidden'
                elif allow == '1':
                    allow = 'Allow'
                elif allow in ['2', '3', '4']:
                    allow = 'Must'
                d['AllowCash'] = allow

                if prcnt.strip() == '':
                    d['CashPercentage'] = 0
                else:
                    d['CashPercentage'] = float(prcnt)
                if fix.strip() == '':
                    d['FixedCash'] = 0
                else:
                    d['FixedCash'] = fix
                stock_list.append(d)
            else:
                [ticker, symbol, shr, allow, prcnt, fix, end] = l.split('|')
                d = dict()
                d["Ticker"] = ticker
                if shr == '        ':
                    shr = '0'
                d['Share'] = int(shr)

                if allow == '0':
                    allow = 'Forbidden'
                elif allow in ['1', '3']:
                    allow = 'Allow'
                elif allow in ['2', '4']:
                    allow = 'Must'
                d['AllowCash'] = allow

                if prcnt.strip() == '':
                    d['CashPercentage'] = 0
                else:
                    d['CashPercentage'] = float(prcnt)
                if fix.strip() == '':
                    d['FixedCash'] = 0
                else:
                    d['FixedCash'] = fix
                stock_list.append(d)

    if etf_dict.has_key('IsPublishIPOV'):
        pass
    else:
        etf_dict['IsPublishIPOV'] = 'True'

    etf_dict['Components'] = stock_list
    etf_dict['FundName'] = ''
    etf_dict['FundManagementCompany'] = ''

    # 深交所可根据pcf文件中的type来判断是否跨市，上交所需通过网络数据来更新
    ticker = etf_dict['Ticker']
    if ticker in instrument_db_dict:
        instrument_db = instrument_db_dict[ticker]
        if instrument_db.exchange_id == 19:
            cross_market = 0
            if ('Type' in etf_dict) and (etf_dict['Type'] == '3'):
                cross_market = 1
            instrument_db.pcf = json.dumps(etf_dict)
            instrument_db.cross_market = cross_market
            instrument_db.prev_nav = nav
            instrument_db.update_date = datetime.now()
            update_instrument_dict[ticker] = instrument_db
        else:
            instrument_db.pcf = json.dumps(etf_dict)
            instrument_db.prev_nav = nav
            instrument_db.update_date = datetime.now()
            update_instrument_dict[ticker] = instrument_db
    else:
        pass


def __read_pcf_file_xml(etf_file_path):
    content_str = open(etf_file_path, 'r')
    etf_dict = dict()
    element_tree = etree.parse(StringIO(content_str.read()))
    filter_title = ['Symbol', 'FundManagementCompany', 'UnderlyingSymbol']
    root = element_tree.getroot()
    nsmap_str = root.nsmap[None]
    for item in root.iterchildren():
        item_title = item.tag.replace('{' + nsmap_str + '}', '')
        data = item.text
        if 'Components' == item_title:
            stock_list = []
            for sub_item in item.iterchildren():
                d = dict()
                d['Share'] = 0
                d['AllowCash'] = 'Allow'
                d['FixedCash'] = 0
                d['CashPercentage'] = 0

                for component in sub_item.iterchildren():
                    component_title = component.tag.replace('{' + nsmap_str + '}', '')
                    component_data = component.text

                    if component_title in filter_title:
                        continue
                    if component_title == 'SubstituteFlag':
                        component_title = 'AllowCash'
                        if component_data == '0':
                            component_data = 'Forbidden'
                        elif component_data in ['1', '3']:
                            component_data = 'Allow'
                        elif component_data in ['2', '4']:
                            component_data = 'Must'
                    elif component_title == 'UnderlyingSecurityID':
                        component_title = 'Ticker'
                    elif component_title == 'ComponentShare':
                        component_title = 'Share'
                        component_data = int(float(component_data))
                    elif component_title == 'PremiumRatio':
                        component_title = 'CashPercentage'
                        component_data = float(component_data) if data != '' else 0
                    elif component_title == 'CreationCashSubstitute':
                        component_title = 'FixedCash'
                        component_data = float(component_data) if data != '' else 0
                    else:
                        continue
                    d[component_title] = component_data
                if 'CashPercentage' not in d:
                    d['CashPercentage'] = 0
                if 'FixedCash' not in d:
                    d['FixedCash'] = 0
                stock_list.append(d)
            continue

        if item_title in filter_title:
            continue
        elif item_title == 'SecurityID':
            item_title = 'Ticker'
            data = data.strip()
        elif item_title == 'UnderlyingSecurityID':
            item_title = 'UnderlyingIndex'
            if data is not None:
                data = data.strip()
        elif item_title == 'Publish':
            item_title = 'IsPublishIPOV'
            data = 'True' if data == 'Y' else 'False'
        elif item_title == 'Creation' or item_title == 'Redemption':
            data = 1 if data == 'Y' else 0
        elif item_title == 'NAV':
            nav = float(data)
        else:
            data = data.strip()
        etf_dict[item_title] = data

    etf_dict['Components'] = stock_list
    etf_dict['FundName'] = ''
    etf_dict['FundManagementCompany'] = ''

    # 深交所可根据pcf文件中的type来判断是否跨市，上交所需通过网络数据来更新
    ticker = etf_dict['Ticker']
    if ticker in instrument_db_dict:
        instrument_db = instrument_db_dict[ticker]
        if instrument_db.exchange_id == 19:
            cross_market = 0
            if ('Type' in etf_dict) and (etf_dict['Type'] == '3'):
                cross_market = 1
            instrument_db.pcf = json.dumps(etf_dict)
            instrument_db.cross_market = cross_market
            instrument_db.prev_nav = nav
            instrument_db.update_date = datetime.now()
            update_instrument_dict[ticker] = instrument_db
        else:
            instrument_db.pcf = json.dumps(etf_dict)
            instrument_db.prev_nav = nav
            instrument_db.update_date = datetime.now()
            update_instrument_dict[ticker] = instrument_db
    else:
        # task_logger.error('unfind ticker:%s' % ticker)
        pass


def __read_pcf_file_csv(csv_file_path):
    print 'read file:%s' % csv_file_path
    csv_file = file(csv_file_path, 'rb')
    reader = csv.reader(csv_file)
    for line in reader:
        if ('000300' == line[0]) and (u'次日权重' == line[5].decode("gbk")):
            ticker = '%06s' % line[2]
            weight = line[14]
            index_SHSZ300List.append([ticker, weight])

        elif (line[0] in '000016|000905') and (u'次日权重' == line[4].decode("gbk")):
            ticker = '%06s' % line[2]
            weight = line[6]
            if '000016' == line[0]:
                index_SSE50List.append([ticker, weight])
            elif '000905' == line[0]:
                index_SH000905List.append([ticker, weight])


def __read_pcf_file_xls(index_file_path):
    data = xlrd.open_workbook(index_file_path)
    table = data.sheets()[0]  # 通过索引顺序获取
    nrows = table.nrows  # 行数
    for i in range(1, nrows, 1):
        if ('000300' == table.cell(i, 0).value) and (u'次日权重' == table.cell(i, 5).value):
            ticker = '%06s' % table.cell(i, 2).value
            weight = table.cell(i, 14).value
            index_SHSZ300List.append([ticker, weight])

        elif (table.cell(i, 0).value in '000016|000905') and (u'次日权重' == table.cell(i, 4).value):
            ticker = '%06s' % table.cell(i, 2).value
            weight = table.cell(i, 14).value
            if '000016' == table.cell(i, 0).value:
                index_SSE50List.append([ticker, weight])
            elif '000905' == table.cell(i, 0).value:
                index_SH000905List.append([ticker, weight])


def _fetch_index(index_list, index_ticker):
    if len(index_list) == 0:
        # print 'Index members is empty, please check the etf file!'
        # email_utils = EmailUtils()
        # email_utils.send_email_operation('ETF Error',
        #                                  'Index:%s members is empty, please check the etf file!' % (index_ticker,))
        return 0

    index_db = instrument_db_dict[index_ticker]
    index_prev_close = Decimal(index_db.prev_close)
    index_mweight = index_db.indx_mweight

    index_member_dict = dict()
    for member_str in index_mweight.split('\n'):
        (ticker, ticker_mweight) = member_str.split('=')
        index_member_dict[ticker] = Decimal(ticker_mweight)

    ticker_list = []
    mweight_list = []
    for (ticker, weight) in index_list:
        instrument_db = instrument_db_dict[ticker]
        ticker_prev_close = instrument_db.prev_close
        index_mweight = index_prev_close * Decimal(weight) / Decimal(ticker_prev_close)
        ticker_list.append(ticker)
        mweight_list.append(ticker + '=' + str('%.6f' % index_mweight))

    a = ';'
    ticker_list.sort()
    index_db.indx_members = a.join(ticker_list)

    a = '\n'
    mweight_list.sort()
    index_db.indx_mweight = a.join(mweight_list)
    index_db.update_date = datetime.now()
    update_instrument_dict[index_ticker] = index_db


def update_by_pcf_file(date_str):
    print 'Enter update_pcf_file.'
    server_host = server_constant_local.get_server_model('host')
    session = server_host.get_db_session('common')
    query = session.query(Instrument)
    for instrument_db in query.filter(Instrument.exchange_id.in_((18, 19))):
        instrument_db_dict[instrument_db.ticker] = instrument_db

    __read_pcf_file(date_str)
    for (ticker, instrument_db) in update_instrument_dict.items():
        session.merge(instrument_db)
    session.commit()
    server_host.close()
    print 'Exit update_pcf_file.'


if __name__ == '__main__':
    options = parse_arguments()
    date_str = options.date
    update_by_pcf_file(date_str)
    # update_by_pcf_file('2017-11-27')

