# -*- coding: utf-8 -*-
# 检测服务器端口和服务是否存在异常
from eod_aps.job import *


ERROR_INFO_LIST = []
email_info_list = []

def __time_to_number(time_str):
    time_items = time_str[11:].split(':')
    return int(time_items[0]) * 60 * 60 + int(time_items[1]) * 60 + int(time_items[2])


def __nanhua_log_monitor_ordgroup(server_model):
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')
    monitor_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                        'grep reject_limit: screenlog_OrdGROUP_%s-*' % today_filter_str.replace('-', '')
    ]
    return_message = server_model.run_cmd_str(';'.join(monitor_cmd_list))
    if return_message == '':
        return

    return_message_items = return_message.split('\r\n')
    return_message_items.remove('')
    return_message = return_message_items[-1]

    start_index = return_message.find(today_filter_str)
    happened_time = return_message[start_index:start_index + 19]
    current_time = date_utils.get_today_str('%Y-%m-%d %H:%M:%S')
    interval_time = __time_to_number(current_time) - __time_to_number(happened_time)
    if interval_time <= 90:
        ERROR_INFO_LIST.append('server:%s screenlog_OrdGROUP Error! Return:%s' % (server_model.name, return_message))


def __nanhua_log_monitor_mktdtcenter(server_model):
    today_filter_str = date_utils.get_today_str('%Y-%m-%d')
    monitor_cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                        'grep _abnormal= screenlog_MktDTCenter_%s-*' % today_filter_str.replace('-', ''),
                        'grep _abnormal= screenlog_MainFrame_%s-*' % today_filter_str.replace('-', '')
    ]
    cmd_return_message = server_model.run_cmd_str(';'.join(monitor_cmd_list))
    if cmd_return_message == '':
        return

    return_message_items = cmd_return_message.split('\n\n')
    return_message_items = sorted(return_message_items, cmp=lambda x, y: cmp(x[1:27], y[1:27]))

    error_message_dict = dict()
    for return_message in return_message_items:
        ticker = return_message.split(',')[0][-6:]

        if 'bid_abnormal' in return_message:
            type_key = 'bid_abnormal'
        elif 'ask_abnormal' in return_message:
            type_key = 'ask_abnormal'
        else:
            continue

        dict_key = '%s|%s' % (ticker, type_key)
        if dict_key in error_message_dict:
            error_message_dict[dict_key].append(return_message.replace('\n', '').replace('\r', ''))
        else:
            error_message_dict[dict_key] = [return_message.replace('\n', '').replace('\r', '')]

    for (dict_key, validate_message_list) in error_message_dict.items():
        temp_error_list = []
        for validate_message in validate_message_list:
            if '_abnormal=true' in validate_message:
                temp_error_list.append(validate_message)
            elif '_abnormal=false' in validate_message:
                temp_error_list = []

        if len(temp_error_list) > 0:
            ERROR_INFO_LIST.append(
                'server:%s screenlog_MktDTCenter Error! Return:%s' % (server_model.name, temp_error_list[-1]))



# 检测南华日志信息
def __nanhua_log_monitor(server_name):
    server_model = server_constant.get_server_model(server_name)
    __nanhua_log_monitor_ordgroup(server_model)
    # __nanhua_log_monitor_mktdtcenter(server_model)


def nanhua_log_monitor_job(server_name):
    # 过滤非交易日和非交易时间
    # if not date_utils.is_trading_day():
    #     return
    #
    # if not date_utils.is_trading_time():
    #     return

    __nanhua_log_monitor(server_name)

    for error_message_info in ERROR_INFO_LIST[:]:
        if error_message_info in email_info_list:
            ERROR_INFO_LIST.remove(error_message_info)
        else:
            email_info_list.append(error_message_info)
    if len(ERROR_INFO_LIST) > 0:
        email_utils2.send_email_group_all('Server Error', '\n'.join(ERROR_INFO_LIST))


if __name__ == '__main__':
    nanhua_log_monitor_job('nanhua')
