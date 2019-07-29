# -*- coding: utf-8 -*-
# 实现对行情重建结果进行校验，并邮件通知
import threading
import traceback

from eod_aps.job import *


def __make_check_result_file(server_model, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y%m%d')

    mktcenter_file_name = server_model.market_file_template.split(',')[0] % date_filter_str
    check_cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder'],
                      'python mktcenter_check_base.py %s %s' % (date_filter_str, mktcenter_file_name)
    ]
    server_model.run_cmd_str(';'.join(check_cmd_list))


def __download_email_result_file(server_model, date_filter_str=None):
    if date_filter_str is None:
        date_filter_str = date_utils.get_today_str('%Y%m%d')

    check_file_name = 'check_result_%s.txt' % (date_filter_str,)
    source_dir = '%s/%s' % (server_model.server_path_dict['mktdtctr_check_folder'], check_file_name)

    mkt_check_file_folder = MKT_CHECK_FILE_FOLDER_TEMPLATE % server_model.name
    target_dir = '%s/%s' % (mkt_check_file_folder, check_file_name)
    server_model.download_file(source_dir, target_dir)

    # 发送邮件通知
    email_content_list = []
    time1_list = []
    time2_list = []

    with open(target_dir, 'rb') as fr:
        for line in fr.readlines():
            if 'mkt_files' in line or 'mkt_num' in line or 'first rebuild' in line:
                email_content_list.append(line)
            if 'time1' in line:
                for line_item in line.split(','):
                    if 'None' in line_item.split(':', 1)[1]:
                        continue
                    if 'time1' in line_item:
                        time1_list.append(line_item.split(':', 1)[1])
                    if 'time2' in line_item:
                        time2_list.append(line_item.split(':', 1)[1])

    subject = '行情重建校验_%s_%s' % (server_model.name, date_filter_str)
    base_content = 'check result file path:\\\\Win-i6qjvbdg2ja\h\data_backup\mkt_files\n\n\nmax_rebuild_time1:%s,max_rebuild_time2:%s' % \
                   (max(time1_list), max(time2_list))

    message_list = []
    message_dict = None
    for str_item in email_content_list:
        if 'mkt_files' in str_item:
            if message_dict is not None:
                message_list.append(message_dict)
            message_dict = dict()
            ticker = str_item.split('/')[-1].split('_')[1]
            message_dict['ticker'] = ticker
        else:
            for temp_str in str_item.split(','):
                temp_item = temp_str.split(':', 1)
                message_dict[temp_item[0]] = temp_item[1]
    message_list.append(message_dict)

    html_title = 'Ticker,first rebuild time1,time2,mkt_num,found_mkt_num,soft_found_mkt_num,rebuild_num,\
matching_rate,time_interval_avg'
    table_list = []
    for item_dict in message_list:
        matching_rate = item_dict.get('matching_rate')
        if matching_rate is None and float(matching_rate.replace('%', '')) < 99:
            matching_rate = '<font color=red>%s</font>' % matching_rate
        table_list.append([item_dict.get('ticker'), item_dict.get('first rebuild time1'), item_dict.get('time2'),
             item_dict.get('mkt_num'), item_dict.get('found_mkt_num'), item_dict.get('soft_found_mkt_num'),
             item_dict.get('rebuild_num'), matching_rate, item_dict.get('time_interval_avg')])
    html_list = email_utils5.list_to_html(html_title, table_list)
    email_utils5.send_email_group_all(subject, base_content + ''.join(html_list), 'html')


def __del_check_result_file(server_model):
    del_cmd_list = ['cd %s' % server_model.server_path_dict['mktdtctr_check_folder'],
                    'rm -rf *.txt'
    ]
    server_model.run_cmd_str(';'.join(del_cmd_list))


def __mktcenter_check(server_name, date_filter_str=None):
    try:
        if date_filter_str is None:
            date_filter_str = date_utils.get_today_str('%Y%m%d')

        server_model = server_constant.get_server_model(server_name)
        __make_check_result_file(server_model, date_filter_str)
        __download_email_result_file(server_model, date_filter_str)
        __del_check_result_file(server_model)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils2.send_email_group_all('[Error]__mktcenter_check:%s.' % server_name, error_msg)


def mktcenter_rebuild_check_job(server_name_tuple):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__mktcenter_check, args=(server_name,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    mktcenter_rebuild_check_job(('huabao',))