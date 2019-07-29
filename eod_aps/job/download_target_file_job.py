# -*- coding: utf-8 -*-
# 之前尝试将日志下载工作安排在15:30，但此时网络流量占用较多，VPN不稳定，容易造成程序崩溃
# 改为定点下载一些时效性比较高的log文件，在下午3:00收盘之后下载，供分析使用，其余log文件还放在夜里下载

import os
import tarfile
import paramiko
from eod_aps.job import *


# target_file_info_list 保存了需要下载的文件信息，每个文件由一个四位的list组成，list的第一位是服务器名
# 第二位是文件所在路径，第三位是一个list，list的每个元素都是一个筛选关键词，第四位是下载的地址
# 下载的时候，会根据服务器和路径找到相应的文件夹，遍历文件夹中的所有文件，只有文件夹中的文件包含所有的筛选元素，以及
# 日期字符串“%Y%m%d”或者“%Y_%m_%d”的时候，才会开始下载该文件

target_file_info_list = [['huabao', '/home/trader/apps/TradePlat/log/',
                         ['StkIntraDayStrategy', 'date_info', '.log'],
                         'Z:/temp/luolinhua/real_log_data/', '%Y%m%d'
                         ],
                         ['huabao', '/home/trader/apps/TradePlat/log/',
                          ['StkIntraDayLeadLagStrategy', 'date_info', '.log'],
                         'Z:/temp/luolinhua/real_log_data/', '%Y%m%d'
                         ]
                        ]


def tar_target_file(server_model, target_file_info, date_str):
    target_file_folder = target_file_info[1]
    key_word_list_temp = target_file_info[2]

    date_info_str = date_utils.datetime_toString(date_utils.string_toDatetime(date_str, '%Y-%m-%d'),
                                                 target_file_info[4])

    key_word_list = []
    for key_word in key_word_list_temp:
        if key_word == 'date_info':
            key_word_list.append(date_info_str)
        else:
            key_word_list.append(key_word)

    t = paramiko.Transport((server_model.ip, server_model.port))
    t.connect(username=server_model.userName, password=server_model.passWord)
    sftp = paramiko.SFTPClient.from_transport(t)
    files = sftp.listdir(target_file_folder)
    file_exist_flag = False
    for f in files:
        if '.tar.gz' in f:
            continue
        # check key word
        key_word_flag = True
        for key_word in key_word_list:
            if key_word not in f:
                key_word_flag = False
                break
        # check date_str
        if key_word_flag:
            file_exist_flag = True

    if file_exist_flag:
        cmd_list = ['cd %s' % target_file_folder,
                    'tar -zcf download_tarfile_%s.tar.gz %s  --exclude=*.tar.gz*' % \
                                                    (date_str, '*%s*' % ('*'.join(key_word_list)))
        ]
        server_model.run_cmd_str(';'.join(cmd_list))

    t.close()
    return file_exist_flag


def download_target_file(server_model, target_file_info, date_str):
    file_name = 'download_tarfile_%s.tar.gz' % date_str
    remote_path = '%s/%s' % (target_file_info[1], file_name)
    dest_path = '%s/%s' % (target_file_info[3], file_name.decode('gb2312'))
    return server_model.download_file(remote_path, dest_path)


def untar(fname, dirs):
    t = tarfile.open(dirs + fname)
    t.extractall(path = dirs)
    t.close()


def uptar_file_download(target_file_info, date_str):
    local_path = target_file_info[3]
    if os.path.exists(local_path + 'download_tarfile_%s.tar.gz' % date_str):
        custom_log.log_info_job('Untar file: download_tarfile_%s.tar.gz' % date_str)
        untar('download_tarfile_%s.tar.gz' % date_str, local_path)


def remove_tar_file_local(target_file_info, date_str):
    local_path = target_file_info[3]
    for i in range(5):
        if os.path.exists(local_path + 'download_tarfile_%s.tar.gz' % date_str):
            custom_log.log_info_job('delete file: %s/download_tarfile_%s.tar.gz' % (local_path, date_str))
            try:
                os.remove(local_path + 'download_tarfile_%s.tar.gz' % date_str)
            except:
                pass
        date_str = date_utils.get_last_trading_day('%Y-%m-%d', date_str)


def remove_tar_file_remote(server_model, target_file_info, date_str):
    cmd_list = ['cd %s' % target_file_info[1],
                'rm -rf download_tarfile_%s.tar.gz' % date_str
    ]
    server_model.run_cmd_str(';'.join(cmd_list))


def download_target_file_job(date_str=None):
    if date_str is None:
        date_str = date_utils.get_today_str('%Y-%m-%d')

    download_flag_all = True
    for target_file_info in target_file_info_list:
        # get server model
        server_name = target_file_info[0]
        server_model = server_constant.get_server_model(server_name)

        # tar target file
        file_exist_flag = tar_target_file(server_model, target_file_info, date_str)

        if file_exist_flag:
            # download file
            download_flag = download_target_file(server_model, target_file_info, date_str)

            # unzip files downloaded
            uptar_file_download(target_file_info, date_str)

            # remove tar files local
            remove_tar_file_local(target_file_info, date_str)

            # remove tar files in server
            remove_tar_file_remote(server_model, target_file_info, date_str)

            if not download_flag:
                download_flag_all = False

    if download_flag_all:
        email_utils2.send_email_group_all('Download target file success!', '')
    else:
        email_utils2.send_email_group_all('[Error]Download target file error!', '')

if __name__ == '__main__':
    download_target_file_job('2017-06-29')