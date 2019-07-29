# -*- coding: utf-8 -*-
from eod_aps.model.schema_jobs import HardWareInfo
from eod_aps.tools.hillstone_utils import query_ip_by_arp
from eod_aps.job import *

# 专线固定IP
filter_arp_ip_list = ['202.104.140.97',]


def ip_report_job():
    arp_ip_list = query_ip_by_arp()
    for ip_item in filter_arp_ip_list:
        arp_ip_list.remove(ip_item)

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    hardware_ip_dict = dict()
    for hardware_info_db in session_job.query(HardWareInfo).filter(HardWareInfo.enable == 1):
        hardware_ip_dict[hardware_info_db.ip] = hardware_info_db
    hardware_ip_list = hardware_ip_dict.keys()
    hardware_ip_list.sort()

    unfind_ip_list = []
    for ip in arp_ip_list:
        if ip not in hardware_ip_list:
            unfind_ip_list.append(ip)

    chang_ip_list = []
    for ip in hardware_ip_list:
        if ip not in arp_ip_list and ip != '':
            hardware_info_db = hardware_ip_dict[ip]
            chang_ip_list.append('IP:%s,User:%s,Describe:%s' % \
                                 (ip, hardware_info_db.user_name, hardware_info_db.describe))

    if len(unfind_ip_list) > 0 or len(chang_ip_list) > 0:
        title_str = u'IP检测报告'
        email_content = u'未登记IP列表:<br>%s<br>未启动IP列表:<br>%s' % ('<br>'.join(unfind_ip_list), '<br>'.join(chang_ip_list))
        email_utils2.send_email_group_all(title_str, email_content, 'html')


if __name__ == '__main__':
    ip_report_job()
