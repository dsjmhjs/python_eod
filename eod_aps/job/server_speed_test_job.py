# coding=utf-8
import os
from eod_aps.job import *

email_content_list = []

speed_test_file_path = '/home/trader/speed_test'
speed_test_file_name = 'speed_test_file.txt'
speed_test_file_name_2 = 'speed_test_file_2.txt'
local_file_folder = DAILY_FILES_TEMP_FOLDER
file_volume = 31032
file_volume_2 = 3983


def remove_local_file():
    if os.path.exists(local_file_folder + speed_test_file_name):
        os.remove(local_file_folder + speed_test_file_name)


def build_local_file():
    if not os.path.exists(local_file_folder):
        os.mkdir(local_file_folder)


def download_target_file(server_model, remote_folder_path, target_file_name, dest_folder_path):
    download_flag = False
    remote_file_path = remote_folder_path + target_file_name
    dest_file_path = dest_folder_path + target_file_name
    if server_model.is_exist(remote_file_path):
        download_flag = server_model.download_file(remote_file_path, dest_file_path)
    return download_flag


def server_speed_test_job(server_name_list):
    email_content = ''
    speed_safe_flag = True
    for server_name in server_name_list:
        server_model = server_constant.get_server_model(server_name)
        remove_local_file()
        build_local_file()
        start_time = date_utils.get_now()

        remote_path = '%s/%s' % (speed_test_file_path, speed_test_file_name)
        dest_path = '%s/%s' % (local_file_folder, speed_test_file_name.decode('gb2312'))
        server_model.download_file(remote_path, dest_path)

        end_time = date_utils.get_now()
        download_time = (end_time - start_time).total_seconds()
        download_speed = file_volume / download_time

        if download_speed < 3000:
            speed_safe_flag = False

        email_content += '%s server download speed test: %s kb/s' % (server_name, int(download_speed))
        email_content += '\n\n'
        email_content += '------------------------------------------------------------------'
        email_content += '\n'

    if speed_safe_flag:
        email_utils2.send_email_group_all('server download speed test', email_content)
    else:
        email_utils2.send_email_group_all('[Error! ]server download speed test', email_content)


def server_speed_monior_guoxin_job():
    email_content = ''
    speed_safe_flag = True
    server_model = server_constant.get_server_model('guoxin')
    remove_local_file()
    build_local_file()
    start_time = date_utils.get_now()
    download_target_file(server_model, speed_test_file_path, speed_test_file_name_2, local_file_folder)
    end_time = date_utils.get_now()
    download_time = (end_time - start_time).total_seconds()
    download_speed = file_volume_2 / download_time

    if download_speed < 500:
        speed_safe_flag = False

    email_content += 'guoxin server download speed test: %s kb/s' % (int(download_speed))
    email_content += '\n\n'
    email_content += '------------------------------------------------------------------'
    email_content += '\n'

    if not speed_safe_flag:
        email_utils2.send_email_group_all('[Error! ]guoxin VPN speed test abnormal', email_content)


if __name__ == '__main__':
    server_speed_test_job(['guoxin', ])