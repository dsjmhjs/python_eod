# -*- coding: cp936 -*-
import os
import xlrd
from eod_aps.model.server_constans import ServerConstant

def list_check(list1, list2):
    if len(list1) != len(list2):
        return True
    else:
        for i in range(len(list1)):
            if abs(list1[i] - list2[i]) > 0.000000001:
                return True
        return False

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

def en_name_convert(en_name):
    if en_name == 'IC':
        return 'sh000905'
    elif en_name == 'IF':
        return 'shsz300'
    elif en_name == 'IH':
        return 'sse50'
    else:
        return en_name.lower()

def commission_list_compare(commission_list1, commission_list2):
    new_commission_list = []
    for i in range(len(commission_list1)):
        new_commission_list.append(max(commission_list1[i], commission_list2[i]))
    return new_commission_list

def get_ticker_name_eng(ticker_name):
    ticker_name_eng = ''
    for n in ticker_name:
        if n.isalpha():
            ticker_name_eng += n
    return ticker_name_eng

def self_check(commission_folder):
    # 找到南华和中信各自的路径
    for commission_file_name in os.listdir(commission_folder):
        if '南华' in commission_file_name:
            nanhua_commissiton_folder = commission_folder + commission_file_name + '/'
        elif '中信' in commission_file_name:
            zhongxin_commissiton_folder = commission_folder + commission_file_name + '/'
        else:
            continue

    # 南华手续费自查
    nanhua_file_path_list = []
    for commission_file_name in os.listdir(nanhua_commissiton_folder):
        nanhua_file_path_list.append(nanhua_commissiton_folder + commission_file_name)

    # 载入一个中英文名转换dict
    cn_name_dict = load_cn_name_dict_nanhua()

    # 载入文件数据并对比
    if_first_file = True
    nanhua_commissiton_dict = dict()
    for commission_file_name in nanhua_file_path_list:
        # 通过第一个文件建立一个dict，之后的都和这个dict进行比较
        if if_first_file:
            fr = xlrd.open_workbook(commission_file_name)
            sh = fr.sheet_by_index(0)
            line_num = sh.nrows
            for line in range(2,line_num):
                ticker_cn_name = sh.cell_value(line, 4)
                ticker_en_name = cn_name_dict[ticker_cn_name]
                commission_list = [sh.cell_value(line, 6), sh.cell_value(line, 7), sh.cell_value(line, 6),
                                   sh.cell_value(line, 7), sh.cell_value(line, 8), sh.cell_value(line, 9), ]
                ticker_en_name = en_name_convert(ticker_en_name)
                if nanhua_commissiton_dict.has_key(ticker_en_name):
                    new_commission_list = commission_list_compare(nanhua_commissiton_dict[ticker_en_name], commission_list)
                    nanhua_commissiton_dict[ticker_en_name] = new_commission_list
                else:
                    nanhua_commissiton_dict[ticker_en_name] = commission_list
            if_first_file = False
        else:
            nanhua_commissiton_dict_compare = dict()
            fr = xlrd.open_workbook(commission_file_name)
            sh = fr.sheet_by_index(0)
            line_num = sh.nrows
            for line in range(2, line_num):
                ticker_cn_name = sh.cell_value(line, 4)
                ticker_en_name = cn_name_dict[ticker_cn_name]
                commission_list = [sh.cell_value(line, 6), sh.cell_value(line, 7), sh.cell_value(line, 6),
                                   sh.cell_value(line, 7), sh.cell_value(line, 8), sh.cell_value(line, 9), ]
                ticker_en_name = en_name_convert(ticker_en_name)
                if nanhua_commissiton_dict_compare.has_key(ticker_en_name):
                    new_commission_list = commission_list_compare(nanhua_commissiton_dict_compare[ticker_en_name], commission_list)
                    nanhua_commissiton_dict_compare[ticker_en_name] = new_commission_list
                else:
                    nanhua_commissiton_dict_compare[ticker_en_name] = commission_list
            for [ticker_en_name, commission_list] in nanhua_commissiton_dict_compare.items():
                if nanhua_commissiton_dict.has_key(ticker_en_name):
                    if nanhua_commissiton_dict[ticker_en_name] == commission_list:
                        pass
                    else:
                        print ticker_en_name, ': different!'
                else:
                        print ticker_en_name, ': no ticker! different! nanhua!'

    # 中信手续费自查
    zhongxin_file_path_list = []
    for commission_file_name in os.listdir(zhongxin_commissiton_folder):
        zhongxin_file_path_list.append(zhongxin_commissiton_folder + commission_file_name)

    if_first_file = True
    zhongxin_commissiton_dict = dict()
    for commission_file_name in zhongxin_file_path_list:
        # 通过第一个文件建立一个dict，之后的都和这个dict进行比较
        if if_first_file:
            fr = xlrd.open_workbook(commission_file_name)
            sh = fr.sheet_by_index(0)
            line_num = sh.nrows
            for line in range(3, line_num):
                ticker_en_name = sh.cell_value(line, 1)
                ticker_en_name = get_ticker_name_eng(ticker_en_name)
                if len(ticker_en_name) != 0:
                    commission_list = [sh.cell_value(line, 4), sh.cell_value(line, 5), sh.cell_value(line, 6),
                                       sh.cell_value(line, 7), sh.cell_value(line, 8), sh.cell_value(line, 9), ]
                    ticker_en_name = en_name_convert(ticker_en_name)
                    if zhongxin_commissiton_dict.has_key(ticker_en_name):
                        new_commission_list = commission_list_compare(zhongxin_commissiton_dict[ticker_en_name],
                                                                      commission_list)
                        zhongxin_commissiton_dict[ticker_en_name] = new_commission_list
                    else:
                        zhongxin_commissiton_dict[ticker_en_name] = commission_list
            if_first_file = False
        else:
            zhongxin_commissiton_dict_compare = dict()
            fr = xlrd.open_workbook(commission_file_name)
            sh = fr.sheet_by_index(0)
            line_num = sh.nrows
            for line in range(3, line_num):
                ticker_en_name = sh.cell_value(line, 1)
                ticker_en_name = get_ticker_name_eng(ticker_en_name)
                if len(ticker_en_name) != 0:
                    commission_list = [sh.cell_value(line, 4), sh.cell_value(line, 5), sh.cell_value(line, 6),
                                       sh.cell_value(line, 7), sh.cell_value(line, 8), sh.cell_value(line, 9), ]
                    ticker_en_name = en_name_convert(ticker_en_name)
                    if zhongxin_commissiton_dict_compare.has_key(ticker_en_name):
                        new_commission_list = commission_list_compare(zhongxin_commissiton_dict_compare[ticker_en_name],
                                                                      commission_list)
                        zhongxin_commissiton_dict_compare[ticker_en_name] = new_commission_list
                    else:
                        zhongxin_commissiton_dict_compare[ticker_en_name] = commission_list
            for [ticker_en_name, commission_list] in zhongxin_commissiton_dict_compare.items():
                if zhongxin_commissiton_dict.has_key(ticker_en_name):
                    if zhongxin_commissiton_dict[ticker_en_name] == commission_list:
                        pass
                    else:
                        print ticker_en_name, ': different!'
                else:
                        print ticker_en_name, ': no ticker! different! zhongxin!'

    return [nanhua_commissiton_dict, zhongxin_commissiton_dict]

def get_guoxin_commission_dict(commission_folder):
    # 找到国信手续费文件的路径
    for commission_file_name in os.listdir(commission_folder):
        if '国信' in commission_file_name:
            guoxin_commissiton_folder = commission_folder + commission_file_name
        else:
            continue

    # 获得国信合约中英文对照表
    cn_name_dict_guoxin = load_cn_name_dict_guoxin()

    guoxin_commissiton_dict = dict()

    fr = xlrd.open_workbook(guoxin_commissiton_folder)
    sh = fr.sheet_by_index(0)
    line_num = sh.nrows
    for line in range(2, line_num):
        ticker_cn_name = sh.cell_value(line, 4)
        ticker_en_name = cn_name_dict_guoxin[ticker_cn_name]
        # if len(sh.cell_value(line, 4)) != 0:
        #     ticker_en_name += str(sh.cell_value(line, 5))
        commission_list = [float(sh.cell_value(line, 6)), float(sh.cell_value(line, 7)), float(sh.cell_value(line, 6)),
                           float(sh.cell_value(line, 7)), float(sh.cell_value(line, 8)), float(sh.cell_value(line, 9)), ]
        ticker_en_name = en_name_convert(ticker_en_name)
        if guoxin_commissiton_dict.has_key(ticker_en_name):
            new_commission_list = commission_list_compare(guoxin_commissiton_dict[ticker_en_name], commission_list)
            guoxin_commissiton_dict[ticker_en_name] = new_commission_list
        else:
            guoxin_commissiton_dict[ticker_en_name] = commission_list

    return guoxin_commissiton_dict


def get_luzheng_commission_dict(commission_folder):
    for commission_file_name in os.listdir(commission_folder):
        if '鲁证' in commission_file_name:
            luzheng_commissiton_folder = commission_folder + commission_file_name
        else:
            continue
    luzheng_commissiton_dict = dict()
    fr = xlrd.open_workbook(luzheng_commissiton_folder)
    sh = fr.sheet_by_index(0)
    line_num = sh.nrows
    for line in range(1, line_num):
        ticker_en_name = sh.cell_value(line, 3).lower()
        if ticker_en_name == 'ic':
            ticker_en_name = 'sh000905'
        elif ticker_en_name == 'if':
            ticker_en_name = 'shsz300'
        elif ticker_en_name == 'ih':
            ticker_en_name = 'sse50'

        commission_list = [float(sh.cell_value(line, 8)), float(sh.cell_value(line, 9)), float(sh.cell_value(line, 8)),
                           float(sh.cell_value(line, 9)), float(sh.cell_value(line, 10)), float(sh.cell_value(line, 11)),]
        if ticker_en_name in luzheng_commissiton_dict:
            new_commission_list = commission_list_compare(luzheng_commissiton_dict[ticker_en_name], commission_list)
            luzheng_commissiton_dict[ticker_en_name] = new_commission_list
        else:
            luzheng_commissiton_dict[ticker_en_name] = commission_list

    return luzheng_commissiton_dict


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

    for (ticker_name_excel, ticker_commission_excel) in sorted(commission_dict.items()):
        #print ticker_name_excel, ticker_commission_excel
        if commission_dict_sql.has_key(ticker_name_excel):
            if list_check(ticker_commission_excel, commission_dict_sql[ticker_name_excel]):
                print 'commission_record_different! %s' % ticker_name_excel
                print "exchange: ", ticker_commission_excel
                print "Mysql: ", commission_dict_sql[ticker_name_excel]
        else:
            print 'commission_record_missed! %s' % ticker_name_excel
            print ticker_commission_excel


def commission_check():
    # 找到手续费文件夹
    for commission_file_name in os.listdir('./'):
        if '手续费' in commission_file_name:
            commission_folder = './' + commission_file_name + '/'
            break

    # 检查统一期货公司各个帐户的手续费是否一致，并得到南华和中信的手续费dict
    [nanhua_commissiton_dict, zhongxin_commissiton_dict] = self_check(commission_folder)
    print 'self check finished!'

    # 获得国信的手续费
    guoxin_commissiton_dict = get_guoxin_commission_dict(commission_folder)

    # 获得鲁证的手续费
    luzheng_commissiton_dict = get_luzheng_commission_dict(commission_folder)

    # 检查我们的手续费表格与上面的是否一致
    server_model_nanhua = ServerConstant().get_server_model('nanhua')
    server_model_zhongxin = ServerConstant().get_server_model('zhongxin')
    server_model_guoxin = ServerConstant().get_server_model('guoxin')
    server_model_luzheng = ServerConstant().get_server_model('luzheng')
    print '\n nanhua:'
    commission_check_sql(server_model_nanhua, nanhua_commissiton_dict)
    print '\n zhongxin:'
    commission_check_sql(server_model_zhongxin, zhongxin_commissiton_dict)
    print '\n guoxin:'
    commission_check_sql(server_model_guoxin, guoxin_commissiton_dict)
    print '\n luzheng:'
    commission_check_sql(server_model_luzheng, luzheng_commissiton_dict)


if __name__ == '__main__':
    commission_check()