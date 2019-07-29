# -*- coding: utf-8 -*-
# 文件上传工具
import os
import shutil
import tarfile
from eod_aps.job import *


def __tar_volume_profile(filter_date_str):
    upload_flag = True
    local_file_path = '%s/%s' % (VOLUME_PROFILE_FOLDER, filter_date_str)
    if not os.path.exists(local_file_path):
        upload_flag = False
        email_utils6.send_email_group_all('[ERROR]Volume Profile File Miss!',
                                          'FilePath:%s is missing!' % local_file_path)
        return upload_flag

    ai_file_name = 'ai_vwap_threshold.csv'
    source_file_path = '%s/%s/%s' % (BASE_STKINTRADAY_CONFIG_FOLDER, filter_date_str, ai_file_name)
    target_file_path = '%s/%s' % (local_file_path, ai_file_name)
    shutil.copyfile(source_file_path, target_file_path)

    tar_file_name = 'volume_profile_%s.tar.gz' % filter_date_str
    tar = tarfile.open(os.path.join(VOLUME_PROFILE_FOLDER, tar_file_name), "w:gz")
    for rt, dirs, files in os.walk(local_file_path):
        for file_name in files:
            if not file_name.endswith('.csv'):
                continue
            file_len = len(open(os.path.join(rt, file_name), 'rU').readlines())
            if file_name.replace('.csv', '').isdigit() and file_len < 240:
                email_utils6.send_email_group_all('[ERROR]Volume Profile File Error!',
                                                  'File:%s len is:%s!' % (file_name, file_len))
                upload_flag = False
                return upload_flag
            full_path = os.path.join(rt, file_name)
            tar.add(full_path, arcname=os.path.join(filter_date_str, file_name))
    tar.close()

    tar_file_size = int(os.path.getsize(os.path.join(VOLUME_PROFILE_FOLDER, tar_file_name)) / 1024)
    if tar_file_size < 6000:
        email_utils6.send_email_group_all('[ERROR]Volume Profile File Error!',
                                          'File:%s size is:%sK!' % (tar_file_name, tar_file_size))
        upload_flag = False
        return upload_flag
    return upload_flag


def __volume_profile_upload(server_name, filter_date_str=None):
    server_model = server_constant.get_server_model(server_name)
    if server_model.type != 'trade_server':
        return

    TRADEPLAT_PROJECT_FOLDER = server_model.server_path_dict['tradeplat_project_folder']
    SERVER_PROFILE_PATH = '%s/volume_profile_datas' % TRADEPLAT_PROJECT_FOLDER

    tar_file_name = 'volume_profile_%s.tar.gz' % filter_date_str
    server_model.upload_file(VOLUME_PROFILE_FOLDER + '/' + tar_file_name,
                             SERVER_PROFILE_PATH + '/' + tar_file_name.decode('gb2312'))
    cmd_list = ['cd %s' % SERVER_PROFILE_PATH,
                'tar -zxf %s' % tar_file_name,
                'rm -rf %s' % tar_file_name,
                'cd %s' % TRADEPLAT_PROJECT_FOLDER,
                'rm volume_profile',
                'ln -s volume_profile_datas/%s volume_profile' % filter_date_str
                ]
    server_model.run_cmd_str(';'.join(cmd_list))


def volume_profile_upload_job(server_name_tuple, filter_date_str=None):
    if filter_date_str is None:
        filter_date_str = date_utils.get_today_str('%Y%m%d')

    upload_flag = __tar_volume_profile(filter_date_str)
    if upload_flag:
        for server_name in server_name_tuple:
            __volume_profile_upload(server_name, filter_date_str)


if __name__ == '__main__':
    volume_profile_upload_job(('huabao', 'guoxin'))
