# coding=utf-8
from eod_aps.tools.tradeplat_order_tools import *
from eod_aps.job import *


def server_order_monitor_job(server_list):
    error_message_list = []
    for server_name in server_list:
        none_order_list = query_none_order(server_name)
        if len(none_order_list) > 0:
            error_message_list.append('[%s] None Order Num:%s' % (server_name, len(none_order_list)))
            for none_order in none_order_list:
                error_message_list.append('%s,%s,%s,%s,%s,%s,%s,%s,%s' % (none_order.id, none_order.order_account,
none_order.strategy_id, none_order.ticker, none_order.order_status, none_order.operation_status, none_order.qty,
none_order.transaction_time, none_order.transaction_time))

    if len(error_message_list) > 0:
        email_utils2.send_email_group_all('[Error]None Order Monitor Report', '\n'.join(error_message_list))


def server_order_constitute_job(server_list):
    table_list = []
    order_status_list = ['Accepted', 'PartialFilled', 'Filled', 'Canceled', 'Rejected', 'Other']
    for server_name in server_list:
        status_constitute_dict = query_order_constitute(server_name)
        tr_list = [server_name]
        for order_status in order_status_list:
            if order_status in status_constitute_dict:
                tr_list.append(str(status_constitute_dict[order_status]))
            else:
                tr_list.append('0')
        table_list.append(tr_list)
    title = 'ServerName,%s' % ','.join(order_status_list)
    html_list = email_utils2.list_to_html(title, table_list)
    email_utils2.send_email_group_all('Order Constitute Report_%s' % date_utils.get_today_str('%Y%m%d%H%M'), ''.join(html_list), 'html')


if __name__ == '__main__':
    # server_order_monitor_job(['huabao', 'guoxin', 'nanhua', 'zhongxin', 'luzheng'])
    server_order_monitor_job(['huabao', ])