# -*- coding: utf-8 -*-
# 版本更新工具
import os
import multiprocessing
import paramiko
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const


server_host = server_constant.get_server_model('host')
LOCAL_EOD_PATH = '%s/eod_aps' % const.EOD_CONFIG_DICT['eod_project_folder']
update_folder_list = ('model', 'server_python', 'tools')


def __upload_file_tools(server_name):
    server_model = server_constant.get_server_model(server_name)
    t = paramiko.Transport((server_model.ip, server_model.port))
    t.connect(username=server_model.userName, password=server_model.passWord)
    sftp = paramiko.SFTPClient.from_transport(t)

    for update_folder_name in update_folder_list:
        local_file_path = '%s/%s' % (LOCAL_EOD_PATH, update_folder_name)
        server_file_path = '%s/%s' % (server_model.server_path_dict['eod_project_folder'], update_folder_name)

        for file_name in os.listdir(local_file_path):
            if '.py' not in file_name:
                continue
            sftp.put(local_file_path + '/' + file_name, server_file_path + '/' + file_name.decode('gb2312'))  # 上传
            print 'Upload file:%s, to:%s success' % (file_name, server_model.name)
    t.close()


def upload_file_tools(server_name_tuple):
    processes = []
    for server_name in server_name_tuple:
        p = multiprocessing.Process(target=__upload_file_tools, args=(server_name,))
        processes.append(p)

    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == '__main__':
    upload_file_tools(('huabao','guoxin','nanhua','zhongxin'))
