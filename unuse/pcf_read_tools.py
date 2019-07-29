# -*- coding: utf-8 -*-
from lxml import etree
import json
from StringIO import StringIO

etf_file_path = 'pcf_159901_20160705.xml'

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
                    else:
                        continue
                    d[component_title] = component_data
                d['FixedCash'] = 0
                stock_list.append(d)
            continue

        if item_title in filter_title:
            continue

        if item_title == 'SecurityID':
            item_title = 'Ticker'
            data = data.strip()
        if item_title == 'Publish':
            item_title = 'IsPublishIPOV'
            data = 'True' if data == 'Y' else 'False'
        if item_title == 'Creation' or item_title == 'Redemption':
            data = 1 if data == 'Y' else 0
        if item_title == 'NAV':
            nav = float(data)

        etf_dict[item_title] = data

    etf_dict['Components'] = stock_list
    etf_dict['FundName'] = ''
    etf_dict['FundManagementCompany'] = ''


if __name__ == '__main__':
    pcf_str =""
    pcf_dict = json.loads(pcf_str)

    ticker_list = []
    for ticker_dict in pcf_dict['Components']:
        ticker = ticker_dict["Ticker"]
        ticker_list.append(ticker)
    print ','.join(ticker_list)
