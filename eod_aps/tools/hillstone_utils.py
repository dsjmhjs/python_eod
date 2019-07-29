# -*- coding: utf-8 -*-
from eod_aps.model.schema_jobs import HardWareInfo
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const
import paramiko
import time
import re
from eod_aps.tools.email_utils import EmailUtils

email_utils = EmailUtils(const.EMAIL_DICT['group2'])


def query_hillstone_flow_info():
    out_info = None
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('172.16.11.1', 22, 'hillstone', 'Yan9sheng', timeout=5)
        remote_conn = client.invoke_shell()
        remote_conn.send('show version \n')
        time.sleep(1)
        out = remote_conn.recv(65535)
        print out

        tftp_cli = 'show statistics-set predef_user_bw current'
        remote_conn.send(tftp_cli + '\n')
        time.sleep(2)
        remote_conn.send('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        time.sleep(1)
        out_info = remote_conn.recv(65535)
        client.close()
    except Exception:
        pass
    return out_info


def build_ip_name_dict():
    ip_name_dict = dict()
    dict_file_path = 'Z:/dailyjob/IP_Person_dict.txt'
    with open(dict_file_path, 'rb') as fr:
        for line in fr.readlines():
            line_item = line.decode('gb2312', 'ignore').replace('\n', '').split(',')
            ip_name_dict[line_item[0]] = line_item[1]
    return ip_name_dict


def format_flow_info(out_info_str):
    ip_name_dict = build_ip_name_dict()
    format_message_list = []
    for line_out_info in out_info_str.split('\n'):
        if 'User' not in line_out_info:
            continue

        reg = re.compile('^(?P<temp>.*)User (?P<ip_info>.*) last 5sec : up(?P<up_info>.*) down (?P<down_info>.*)')
        reg_match = reg.match(line_out_info)
        line_dict = reg_match.groupdict()

        ip = line_dict['ip_info'].strip()
        name = ''
        if ip in ip_name_dict:
            name = ip_name_dict[ip]

        up_info = line_dict['up_info'].strip()
        down_info = line_dict['up_info'].strip()
        format_message_list.append((ip, name, __speed_format(up_info), __speed_format(down_info)))
    format_message_list.sort(key=lambda obj: float(obj[-1]), reverse=True)
    if len(format_message_list) > 10:
        format_message_list = format_message_list[:10]
    return format_message_list


def __speed_format(speed_str):
    try:
        return '%.2f' % (float(speed_str) / 1048576)
    except ValueError:
        return '0'


def query_ip_flow_info():
    out_info_str = query_hillstone_flow_info()
    format_message_list = format_flow_info(out_info_str)
    html_list = email_utils.list_to_html(u'IP,用户,上传速度(M),下载速度(M)', format_message_list)
    return html_list


def email_ip_flow():
    html_list = query_ip_flow_info()
    email_utils.send_email_group_all(u'网络报告', ''.join(html_list), 'html')


def __query_hillstone_arp():
    out_info = None
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('172.16.11.1', 2223, 'hillstone', 'Yan9sheng', timeout=5)
        remote_conn = client.invoke_shell()
        remote_conn.send('show version \n')
        time.sleep(1)
        out = remote_conn.recv(65535)

        arp_cli = 'show arp'
        remote_conn.send(arp_cli + '\n')
        time.sleep(2)
        remote_conn.send('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n'
                         '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        time.sleep(1)
        out_info = remote_conn.recv(65535)
        client.close()
    except Exception:
        pass
    return out_info


def format_ip_info(out_info_str):
    ip_list = []
    for line_out_info in out_info_str.split('\n'):
        if 'ARPA' not in line_out_info:
            continue

        reg = re.compile('^(?P<temp>.*)Internet (?P<ip_info>[^ ]*).*')
        reg_match = reg.match(line_out_info)
        line_dict = reg_match.groupdict()

        ip = line_dict['ip_info'].strip()
        ip_list.append(ip)
    return ip_list


def query_ip_by_arp():
    out_info_str = __query_hillstone_arp()
    ip_list = format_ip_info(out_info_str)
    ip_list.sort()
    return ip_list


if __name__ == '__main__':
    arp_ip_list = query_ip_by_arp()

    server_model = server_constant.get_server_model('host')
    session_job = server_model.get_db_session('jobs')

    hardware_ip_list = []
    for hardware_info_db in session_job.query(HardWareInfo):
        hardware_ip_list.append(hardware_info_db.ip)

    # for ip in arp_ip_list:
    #     if ip not in hardware_ip_list:
    #         print ip

    for ip in hardware_ip_list:
        if ip not in arp_ip_list:
            print ip
