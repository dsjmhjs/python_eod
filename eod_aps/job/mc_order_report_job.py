# -*- coding: utf-8 -*-
# 解析TradePlat日志，生成mc每日成交报告
import os
import re
from collections import OrderedDict
from eod_aps.job import *


order_status_dict = {'-1': 'None', '0': 'New', '1': 'PartialFilled', '2': 'Filled', '3': 'DoneForDay',
                     '4': 'Canceled', '5': 'Replace', '6': 'PendingCancel', '7': 'Stopped', '8': 'Rejected',
                     '9': 'Suspended', '10': 'PendingNew', '11': 'Calculated', '12': 'Expired',
                     '13': 'AcceptedForBidding','14': 'PendingReplace', '15': 'EndAsSucceed',
                     '16': 'Accepted', '17': 'InternalRejected'}


class Mc_Order(object):
    """
        mc的order解析
    """
    InvestorID = None
    CreationTime = None
    Target = None
    Price = None
    Qty = None
    Status = None
    TransactionTime = ''
    ExQty = 0
    CancelQty = 0
    ExAvgPrice = 0
    MCOrdID = None
    YSOrdID = ''
    Comment = ''

    def __init__(self):
        pass


def __analysis_placeorder(line_str):
    line_str = line_str.replace('>', '')
    reg = re.compile('^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<message_type>.*)\] (?P<order_type>[^,]*), investor=(?P<investor>[^ ]*), ticker=(?P<ticker>[^,]*), cli_ordid=(?P<cli_ordid>[^ ]*), hedgeflag=(?P<hedgeflag>[^ ]*), direction=(?P<direction>[^ ]*), price=(?P<price>[^ ]*), qty=(?P<qty>[^ ]*)')
    regMatch = reg.match(line_str)
    line_dict = regMatch.groupdict()
    return line_dict


def __analysis_orderreport(line_str):
    reg = re.compile('^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<message_type>.*)\] (?P<order_type>[^ ]*): ordid=(?P<ordid>[^ ]*), cli_ordid=(?P<cli_ordid>[^,]*), status=(?P<status>[^ ]*), trade_price=(?P<trade_price>[^ ]*), trade_qty=(?P<trade_qty>[^ ]*), ex_qty=(?P<ex_qty>[^ ]*), ex_avg_price=(?P<ex_avg_price>[^ ]*), canceled_qty=(?P<canceled_qty>[^ ]*), is_end=(?P<is_end>[^ ]*)')
    regMatch = reg.match(line_str)
    line_dict = regMatch.groupdict()
    return line_dict


def __analysis_order_rejected(line_str):
    reg = re.compile('^.*\[(?P<date>.*)\] \[(?P<from>.*)\] \[(?P<message_type>.*)\] (?P<order_type>.*), cli_ordid=(?P<cli_ordid>[^,]*), (?P<comment>[^,]*)')
    regMatch = reg.match(line_str)
    line_dict = regMatch.groupdict()
    return line_dict


def __download_mc_log_file(server_name):
    now_date_str = date_utils.get_today_str('%Y%m%d')
    last_trading_day = date_utils.get_last_trading_day('%Y-%m-%d').replace('-', '')
    start_filter_str = last_trading_day + '-200000'
    end_filter_str = now_date_str + '-160000'

    local_log_save_folder = LOG_BACKUP_FOLDER_TEMPLATE % server_name

    server_model = server_constant.get_server_model(server_name)
    server_file_path = server_model.server_path_dict['tradeplat_log_folder']
    folder_file_list = server_model.list_dir(server_file_path)
    oma_log_file_list = []
    for log_file_name in folder_file_list:
        if 'screenlog_OMA' not in log_file_name:
            continue
        filter_key = log_file_name.split('.')[0].split('_')[2]
        if start_filter_str < filter_key < end_filter_str:
            server_model.download_file(server_file_path + '/' + log_file_name,
                                       local_log_save_folder + '/' + log_file_name)
            oma_log_file_list.append(log_file_name)
    oma_log_file_list.sort()
    return oma_log_file_list


def mc_order_report_job(server_name):
    log_file_list = __download_mc_log_file(server_name)
    if len(log_file_list) == 0:
        return

    mc_order_dict = OrderedDict()
    local_log_save_folder = LOG_BACKUP_FOLDER_TEMPLATE % server_name
    for log_file_name in log_file_list:
        log_file_path = os.path.join(local_log_save_folder, log_file_name)
        with open(log_file_path, 'rb') as fr:
            for line_str in fr.readlines():
                if 'place order,' in line_str and 'investor' in line_str:
                    line_dict = __analysis_placeorder(line_str)
                    mc_order = Mc_Order()
                    mc_order.InvestorID = line_dict['investor']
                    mc_order.CreationTime = line_dict['date']
                    mc_order.Target = line_dict['ticker']
                    mc_order.Price = line_dict['price']
                    mc_order.Qty = int(line_dict['qty']) * int(line_dict['direction'])
                    mc_order.MCOrdID = line_dict['cli_ordid']
                    mc_order_dict[mc_order.MCOrdID] = mc_order
                elif 'OrderReport:' in line_str and 'is_end=true' in line_str:
                    line_dict = __analysis_orderreport(line_str)
                    mcord_id = line_dict['cli_ordid']
                    if mcord_id not in mc_order_dict:
                        custom_log.log_error_job('error mcord_id:', mcord_id)
                        continue
                    mc_order = mc_order_dict[mcord_id]
                    mc_order.Status = order_status_dict[line_dict['status']]
                    mc_order.TransactionTime = line_dict['date']
                    mc_order.ExQty = line_dict['ex_qty']
                    mc_order.CancelQty = line_dict['canceled_qty']
                    mc_order.ExAvgPrice = line_dict['ex_avg_price']
                    mc_order.YSOrdID = line_dict['ordid']
                elif 'Place Rejected,' in line_str:
                    line_dict = __analysis_order_rejected(line_str)
                    mcord_id = line_dict['cli_ordid']
                    if mcord_id not in mc_order_dict:
                        custom_log.log_error_job('error mcord_id:', mcord_id)
                        continue
                    mc_order = mc_order_dict[mcord_id]
                    mc_order.Status = 'Rejected'
                    mc_order.Comment = line_dict['comment']

    html_title = 'InvestorID,CreationTime,Target,Price,Qty,Status,TransactionTime,ExQty,CancelQty,\
ExAvgPrice,MCOrdID,YSOrdID,Comment'
    table_list = []
    for (key, item_value) in mc_order_dict.items():
        table_list.append([item_value.InvestorID ,item_value.CreationTime,item_value.Target,item_value.Price,
                           item_value.Qty,item_value.Status,item_value.TransactionTime,item_value.ExQty,
                           item_value.CancelQty,item_value.ExAvgPrice,item_value.MCOrdID,item_value.YSOrdID,
                           item_value.Comment])
    html_list = email_utils10.list_to_html(html_title, table_list)
    email_utils10.send_email_group_all(unicode('MC每日成交报告_%s', 'utf-8') % date_utils.get_today_str('%Y%m%d'), ''.join(html_list), 'html')


if __name__ == '__main__':
    mc_order_report_job('guoxin')

