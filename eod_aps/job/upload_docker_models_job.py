# -*- coding: utf-8 -*-
# 上传文件至托管服务器FTP
import os
import tarfile
import threading
import traceback
from eod_aps.job import *
from eod_aps.model.schema_jobs import DockerModelTicker
from eod_aps.tools.md5_check_utils import file_md5_check, get_local_file_md5, get_file_size

email_utils = EmailUtils(const.EMAIL_DICT['group2'])
config_item_template1 = """\
model_config_list: {
%s
}
"""
config_item_template2 = """\
    config:{
        model_platform:"tensorflow",
        name:"%s",
        base_path:"/models/%s",
        model_version_policy:{
            all:{}
        }
    }\
"""


class UploadDockerModelFiles(object):
    def __init__(self, target_servers, index_num, date_str=None):
        if date_str is None:
            date_str = date_utils.get_next_trading_day('%Y%m%d')
        self.__index_num = index_num
        self.__date_str = date_str
        self.__target_servers = target_servers

        self.__model_ticker_list = []
        self.__tar_folder_list = []
        self.__upload_result_dict = dict()

        # 使用的文件目录
        self.__stock_model_folder = '%s/%s/MLP-1T-regression' % (BASE_STKINTRADAY_MODEL_FOLDER, self.__date_str)
        self.__index_model_folder = '%s/%s/MLP-1T-regression' % (BASE_INTRADAY_INDEX_MODEL_FOLDER, self.__date_str)
        self.__tar_file_name = 'tradeplat_%s_%s.tar.gz' % (self.__date_str, self.__index_num)

        self.__model_file_name = 'model_config'
        self.__target_model_file_path = '%s/%s' % (DAILY_FILES_FOLDER, self.__model_file_name)
        self.__target_tar_file_path = '%s/%s' % (DAILY_FILES_FOLDER, self.__tar_file_name)

    def upload_models_files(self, stock_include_flag=True, index_include_flag=True, model_flag=True, tar_flag=True):
        """
        :param stock_include_flag:打包是否包含股票models文件
        :param index_include_flag:打包是否包含股指models文件
        :param model_flag:是否重新生成model_config文件
        :param tar_flag:是否重新压缩
        """
        self.__pre_read_models(stock_include_flag, index_include_flag)

        if model_flag:
            self.__build_model_file()
        if tar_flag:
            self.__tar_docker_models(model_flag)

        local_md5_value = get_local_file_md5(self.__target_tar_file_path)
        local_size = get_file_size(self.__target_tar_file_path)

        threads = []
        for server_name in self.__target_servers:
            t = threading.Thread(target=self.__upload_docker_models_job, args=(server_name,))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.__send_result_email(local_size, local_md5_value)

    def __pre_read_models(self, stock_include_flag, index_include_flag):
        # 预检查需要打包的文件和ticker列表
        if stock_include_flag:
            for sub_folder_name in os.listdir(self.__stock_model_folder):
                sub_folder_path = os.path.join(self.__stock_model_folder, sub_folder_name)
                if not os.path.isdir(sub_folder_path):
                    continue
                self.__model_ticker_list.append(sub_folder_name)
                self.__tar_folder_list.append(sub_folder_path)

        if index_include_flag:
            for sub_folder_name in os.listdir(self.__index_model_folder):
                sub_folder_path = os.path.join(self.__index_model_folder, sub_folder_name)
                if not os.path.isdir(sub_folder_path):
                        continue
                self.__model_ticker_list.append(sub_folder_name)
                self.__tar_folder_list.append(sub_folder_path)

    def __build_model_file(self):
        # 生成model文件
        config_content_list = []
        for ticker in self.__model_ticker_list:
            config_item = config_item_template2 % (ticker, ticker)
            config_content_list.append(config_item)
        config_content_list.sort()
        content_str = config_item_template1 % ('\n\n'.join(config_content_list))
        with open(self.__target_model_file_path, 'w+') as fr:
            fr.write(content_str)

    def __tar_docker_models(self, model_flag):
        tar = tarfile.open(self.__target_tar_file_path, "w:gz")
        if model_flag:
            tar.add(self.__target_model_file_path, arcname=os.path.join('models', self.__model_file_name))

        for sub_folder_path in self.__tar_folder_list:
            sub_folder = os.path.dirname(sub_folder_path)
            for root, dir_str, files in os.walk(sub_folder_path):
                root_ = os.path.relpath(root, start=sub_folder)
                for file_name in files:
                    full_path = os.path.join(root, file_name)
                    tar.add(full_path, arcname=os.path.join('models', root_, file_name))
        tar.close()

    def __upload_docker_models_job(self, server_name):
        try:
            upload_flag, file_size = self.__upload_tradeplat_file(server_name)
            self.__upload_result_dict[server_name] = (upload_flag, file_size)
        except Exception:
            error_msg = traceback.format_exc()
            custom_log.log_error_job(error_msg)
            email_utils2.send_email_group_all('[Error]__upload_docker_models_job:%s.' % server_name, error_msg)

    def __upload_tradeplat_file(self, server_name):
        tradeplat_file_name = os.path.basename(self.__target_tar_file_path)
        source_file_path = self.__target_tar_file_path

        server_model = server_constant.get_server_model(server_name)
        # 上传压缩包
        if server_model.type != 'trade_server':
            target_folder = '%s/%s' % (server_model.ftp_upload_folder, self.__date_str)
            if not server_model.is_exist(target_folder):
                server_model.mkdir(target_folder)

            target_file_path = '%s/%s' % (target_folder, tradeplat_file_name)
            server_model.upload_file(source_file_path, target_file_path)

            upload_flag = server_model.is_exist(target_file_path)
            file_size = server_model.get_size(target_file_path)
        else:
            server_tradeplat_folder = server_model.server_path_dict['tradeplat_project_folder']
            target_file_path = '%s/%s' % (server_tradeplat_folder, tradeplat_file_name)
            server_model.upload_file(source_file_path, target_file_path)

            upload_flag = file_md5_check(server_model, target_file_path, source_file_path)
            file_size = server_model.get_size(target_file_path)

            run_cmd_list = ['cd %s' % server_tradeplat_folder,
                            'rm -rf ./models',
                            'tar -zxf %s' % tradeplat_file_name,
                            'rm -rf *.tar.gz']
            server_model.run_cmd_str(';'.join(run_cmd_list))
        file_size = '%.2f' % (float(file_size) / float(1024 * 1024 * 1024))
        return upload_flag, file_size

    def __send_result_email(self, local_size, local_md5_value):
        table_list = [['Local', local_size, local_md5_value, '']]

        for server_name in self.__upload_result_dict.keys():
            upload_flag, size = self.__upload_result_dict[server_name]
            if not upload_flag:
                upload_flag = '%s(Error)' % str(upload_flag)
            table_list.append([server_name, size, '', str(upload_flag)])

        html_list = email_utils.list_to_html('Server,Size(G),MD5,Upload_Flag', table_list)
        email_utils.send_email_group_all(u'日内Models文件上传結果报告', ''.join(html_list), 'html')


if __name__ == '__main__':
    deposit_servers = server_constant.get_deposit_servers()
    upload_docker_model_files = UploadDockerModelFiles(('zhongtai', ), 1)
    upload_docker_model_files.upload_models_files(stock_include_flag=False, model_flag=False)
