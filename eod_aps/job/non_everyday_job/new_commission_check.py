# -*- coding: cp936 -*-
import os
import xlrd
import re
from eod_aps.model.server_constans import ServerConstant


def list_check(list1, list2):
    if len(list1) != len(list2):
        return True
    else:
        for i in range(len(list1)):
            # print list2[i]
            if abs(float(list1[i]) - float(list2[i])) > 0.000000001:
                return True
        return False


def en_name_convert(en_name):
    if en_name == 'IC':
        return 'sh000905'
    elif en_name == 'IF':
        return 'shsz300'
    elif en_name == 'IH':
        return 'sse50'
    else:
        return en_name.lower()


def load_cn_name_dict_nanhua():
    cn_name_dict_nanhua = dict()
    fr = open('./ticker_cn_name_nanhua.csv')
    for line in fr.readlines():
        name_cn = line.split(',')[0]
        name_cn = unicode(name_cn, 'gbk')
        name_en = line.split(',')[1].strip()
        cn_name_dict_nanhua[name_cn] = name_en
    return cn_name_dict_nanhua


def load_cn_name_dict_guoxin():
    cn_name_dict_guoxin = dict()
    fr = open('./ticker_cn_name_guoxin.csv')
    for line in fr.readlines():
        name_cn = line.split(',')[0]
        name_cn = unicode(name_cn, 'gbk')
        name_en = line.split(',')[1].strip()
        cn_name_dict_guoxin[name_cn] = name_en
    return cn_name_dict_guoxin


def get_exchange_commission_dict(execl_path):
    exchange_commission_dict = dict()
    fr = xlrd.open_workbook(execl_path)
    sheet = fr.sheet_by_index(0)
    line_num = sheet.nrows
    for line in range(2, line_num):
        try:
            ticker_type = re.search('\w+', sheet.cell_value(line, 2)).group()
        except Exception as e:
            continue
        ticker_type_num = sheet.cell_value(line, 3)
        if ticker_type_num.isdigit():
            ticker_type = en_name_convert(ticker_type + ticker_type_num)
        open_ratio = sheet.cell_value(line, 5)
        if type(open_ratio) is float or type(open_ratio) is int:
            open_ratio_by_money = open_ratio
            open_ratio_by_volume = 0
            close_ratio_by_money = open_ratio_by_money
            close_ratio_by_volume = 0
        else:
            if u'手' in open_ratio:
                open_ratio_by_money = 0

                open_ratio_by_volume = re.search(r'\d+(\.\d+)?', open_ratio.encode('utf-8')).group()
                close_ratio_by_money = 0
                close_ratio_by_volume = open_ratio_by_volume
            else:
                open_ratio_by_money = re.search(r'\d+(\.\d+)?', str(open_ratio)).group()
                open_ratio_by_volume = 0
                close_ratio_by_money = open_ratio_by_money
                close_ratio_by_volume = 0
        today_ratio = sheet.cell_value(line, 6)
        if type(today_ratio) is float or type(today_ratio) is int:
            close_today_ratio_by_money = today_ratio
            close_today_ratio_by_volume = 0
        else:
            if u'手' in today_ratio:
                close_today_ratio_by_money = 0
                close_today_ratio_by_volume = re.search(r'\d+(\.\d+)?', today_ratio).group()
            else:
                close_today_ratio_by_money = re.search(r'\d+(\.\d+)?', today_ratio).group()
                close_today_ratio_by_volume = 0
        exchange_commission_dict[ticker_type] = [open_ratio_by_money, open_ratio_by_volume, close_ratio_by_money,
                                                 close_ratio_by_volume, close_today_ratio_by_money,
                                                 close_today_ratio_by_volume]
    return exchange_commission_dict


def get_zhongxin_commission_dict(execl_path):
    commission_dict = dict()
    fr = xlrd.open_workbook(execl_path)
    sheet = fr.sheet_by_index(0)
    line_num = sheet.nrows
    for line in range(3, line_num):
        ticker_type = en_name_convert(sheet.cell_value(line, 1))
        if ticker_type:
            open_ratio_by_money = sheet.cell_value(line, 4)
            open_ratio_by_volume = sheet.cell_value(line, 5)
            close_ratio_by_money = sheet.cell_value(line, 6)
            close_ratio_by_volume = sheet.cell_value(line, 7)
            close_today_ratio_by_money = sheet.cell_value(line, 8)
            close_today_ratio_by_volume = sheet.cell_value(line, 9)
            commission_dict[ticker_type] = [open_ratio_by_money, open_ratio_by_volume, close_ratio_by_money,
                                            close_ratio_by_volume, close_today_ratio_by_money,
                                            close_today_ratio_by_volume]

    return commission_dict


def get_luzheng_commission_dict(execl_path):
    commission_dict = dict()
    fr = xlrd.open_workbook(execl_path)
    sheet = fr.sheet_by_index(0)
    line_num = sheet.nrows
    for line in range(2, line_num):
        ch_ticker_name = sheet.cell_value(line, 4)
        ticker_type = en_name_convert(sheet.cell_value(line, 5))
        if u'期权' in ch_ticker_name:
            ticker_type = 'option_' + ticker_type
        direction = sheet.cell_value(line, 6)
        if u'平' == direction:
            close_ratio_by_money = sheet.cell_value(line, 7)
            close_ratio_by_volume = sheet.cell_value(line, 8)
            close_today_ratio_by_money = sheet.cell_value(line, 9)
            close_today_ratio_by_volume = sheet.cell_value(line, 10)
            if ticker_type in commission_dict and len(commission_dict[ticker_type]) == 2:
                commission_dict[ticker_type].extend([close_ratio_by_money, close_ratio_by_volume,
                                                     close_today_ratio_by_money, close_today_ratio_by_volume])
            else:
                commission_dict[ticker_type] = [close_ratio_by_money, close_ratio_by_volume, close_today_ratio_by_money,
                                                close_today_ratio_by_volume]
        else:
            open_ratio_by_money = sheet.cell_value(line, 7)
            open_ratio_by_volume = sheet.cell_value(line, 8)
            if ticker_type in commission_dict and len(commission_dict[ticker_type]) == 4:
                commission_dict[ticker_type].insert(0, open_ratio_by_money)
                commission_dict[ticker_type].insert(1, open_ratio_by_volume)
            else:
                commission_dict[ticker_type] = [open_ratio_by_money, open_ratio_by_volume]
    return commission_dict


def get_commission_dict_bymultiple(exchange_commission_dict, multiple):
    commission_dict = dict()
    for k, v in exchange_commission_dict.items():
        n_v = []
        for i in v:
            n_v.append(float(i) * float(multiple))
            commission_dict[k] = n_v
    return commission_dict


def get_abs_filepath(commission_folder):
    abs_filepath_list = []
    for file_name in os.listdir(commission_folder):
        abs_filepath = os.path.join(commission_folder, file_name)
        abs_filepath_list.append(abs_filepath)
    return abs_filepath_list


def format_dict(commission_dict):
    new_commission_dict = dict()
    for k, v in commission_dict.items():
        k = k.strip()
        n_v = []
        for i in v:
            if type(i) is unicode:
                n_v.append(i.strip())
            else:
                n_v.append(i)
        new_commission_dict[k] = n_v
    return new_commission_dict


def dict_check(f_d, s_d):
    if len(f_d) != len(s_d):
        return False
    for k, v in f_d.items():
        if k in s_d:
            if list_check(v, s_d[k]):
                return False
            else:
                return True
    return True


def commission_check_sql(server_model, commission_dict):
    session_portfolio = server_model.get_db_session('common')
    query_sql = "select * from common.instrument_commission_rate;"
    commission_query = session_portfolio.execute(query_sql)
    commission_dict_sql = dict()
    for commission in commission_query:
        ticker_en_name = commission[0]
        ticker_en_name = en_name_convert(ticker_en_name)
        ticker_commission_sql_temp = commission[1:]
        ticker_commission_sql = []
        for commission_number in ticker_commission_sql_temp:
            ticker_commission_sql.append(float(commission_number))
        commission_dict_sql[ticker_en_name] = ticker_commission_sql
    different_list = []
    missed_list = []
    for (ticker_name_excel, ticker_commission_excel) in sorted(commission_dict.items()):

        if commission_dict_sql.has_key(ticker_name_excel):
            if list_check(ticker_commission_excel, commission_dict_sql[ticker_name_excel]):
                different_list.append(
                    (ticker_name_excel, ticker_commission_excel, commission_dict_sql[ticker_name_excel]))
        else:
            missed_list.append((ticker_name_excel, ticker_commission_excel))
    print 'different :'
    for i in different_list:
        print i
    print 'missed_list :'
    for i in missed_list:
        print i


def commission_check(base_path):
    exchange_folder = os.path.join(base_path, u'交易所')
    exchange_commission_file = get_abs_filepath(exchange_folder)[0]
    exchange_commission_dict = get_exchange_commission_dict(exchange_commission_file)
    for folder_name in os.listdir(base_path):
        flag = True
        if u'中信' in folder_name:
            commission_folder = os.path.join(base_path, folder_name)

            file_list = get_abs_filepath(commission_folder)
            if len(file_list) > 1:
                f_commission_dict = get_zhongxin_commission_dict(file_list[0])
                for i in file_list[1:]:
                    s_commission_dict = get_zhongxin_commission_dict(i)
                    if not dict_check(f_commission_dict, s_commission_dict):
                        flag = False
                        break
            if flag:
                commission_dict = get_zhongxin_commission_dict(file_list[0])
                server_name = 'zhongxin'
        elif u'南华' in folder_name:
            commission_folder = os.path.join(base_path, folder_name)
            commission_dict = get_commission_dict_bymultiple(exchange_commission_dict, 1.05)
            server_name = 'nanhua'
        elif u'鲁证' in folder_name:
            commission_folder = os.path.join(base_path, folder_name)
            server_name = 'luzheng'
        elif u'兴证' in folder_name:
            commission_folder = os.path.join(base_path, folder_name)
            commission_dict = get_commission_dict_bymultiple(exchange_commission_dict, 1.05)
            server_name = 'xinzheng'
        else:
            continue
        server_model = ServerConstant().get_server_model(server_name)

        if flag:
            print server_name, ': '
            commission_check_sql(server_model, commission_dict)


if __name__ == '__main__':
    path = u'D:\work\手续费'
    # # print get_exchange_commission_dict(path)
    # print format_dict(get_luzheng_commission_dict(path))
    commission_check(path)
