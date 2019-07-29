# -*- coding: utf-8 -*-
import xlrd
import re
from eod_aps.tools.date_utils import DateUtils

base_file_path = 'Z:/temp/yangzhoujie/ib'
date_utils = DateUtils()

security_dict = {'IQEl': 'IQE LN',
                 'ALGN': 'ALGN US',
                 'BA': 'BA US',
                 'DWDP': 'DWDP US',
                 'IBKR': 'IBKR US',
                 'MCD': 'MCD US',
                 'MMM': 'MMM US',
                 'UNH': 'UNH US',
                 'HHIG8': 'HHIG8 HK'}
Side_dict = {'(Sold)': 'Sell',
             '(Bought)': 'Buy'
             }


def __write_output_file(input_list):
    index_number = 1
    date_format_str = date_utils.get_today_str()[2:]

    save_content_list = []
    for input_item in input_list:
        row_list = []

        reg = re.compile('^Total (?P<security_type>.*) (?P<side_type>[^ ]*)')
        reg_name_dict = reg.match(input_item[0]).groupdict()
        row_list.append(security_dict[reg_name_dict['security_type']])
        row_list.append(Side_dict[reg_name_dict['side_type']])

        row_list.append('%s' % abs(int(input_item[7])))
        row_list.append('%s' % abs(int(input_item[7])))
        row_list.append('%s' % input_item[8])

        row_list.append('IBKR')
        row_list.append('DCSG')
        row_list.append('IBKRH')
        row_list.append('DefaultCashStrategy')
        row_list.append('')

        row_list.append(input_item[-2])
        row_list.append(input_item[-1])

        row_list.append('MTF UDS#4')
        row_list.append('U9599886')

        row_list.append('%s00%s' % (date_format_str, str(index_number).zfill(2)))

        row_list.append('MTF UDN#1')
        row_list.append('%s' % abs(float(input_item[10])))
        row_list.append('MTF UDN#2')
        row_list.append('%s' % abs(float(input_item[11])))
        row_list.append('MTF UDN#3')
        row_list.append('0')
        # print row_list
        save_content_list.append(','.join(row_list))
        index_number += 1

    save_title = 'security,Side,Amount,Done,Price,Broker,Prt,Cust,Strategy1,Note, TradeDate,SettleDate, Category1,\
Value1, Tnum1,Category2,Value2,Category3,Value3,Category4,Value4'
    save_content_list.insert(0, save_title)
    output_file_path = '%s/trade_file_test.csv' % base_file_path
    with open(output_file_path, 'w+') as fr:
        fr.write('\n'.join(save_content_list))


def __read_input_file():
    return_list = []
    input_file_path = '%s/IB2.6(2).xlsx' % base_file_path
    data = xlrd.open_workbook(input_file_path)
    table = data.sheets()[0]  # 通过索引顺序获取
    nrows = table.nrows  # 行数

    format_trade_date = None
    format_settle_date = None
    for i in range(2, nrows, 1):
        account_id_cell = table.cell(i, 0).value
        if 'Total' in account_id_cell and ('Sold' in account_id_cell or 'Bought' in account_id_cell):
            row_value_list = table.row_values(i)
            row_value_list.append(format_trade_date)
            row_value_list.append(format_settle_date)
            return_list.append(row_value_list)
            # print row_value_list
        else:
            trade_date = table.cell(i, 3).value
            settle_date = table.cell(i, 4).value
            if isinstance(settle_date, float):
                if int(trade_date[12:14]) >= 12:
                    temp_trade_date = date_utils.string_toDatetime(trade_date, '%Y-%m-%d, %H:%M:%S')
                    format_trade_date = date_utils.get_last_date(1, temp_trade_date).strftime('%d/%m/%Y')
                else:
                    temp_trade_date = date_utils.string_toDatetime(trade_date, '%Y-%m-%d, %H:%M:%S')
                    format_trade_date = temp_trade_date.strftime('%d/%m/%Y')
                format_settle_date = xlrd.xldate.xldate_as_datetime(settle_date, 0).strftime('%d/%m/%Y')
    return return_list


if __name__ == '__main__':
    return_list = __read_input_file()
    __write_output_file(return_list)
