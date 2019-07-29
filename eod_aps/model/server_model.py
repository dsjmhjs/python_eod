# -*- coding: utf-8 -*-
import traceback
import paramiko
import socket
from eod_aps.model.custom_ftp_model import SftpModel, FtpModel, WsdlFtpModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


class ServerModel(object):
    session_pool = []

    def __init__(self, name):
        self.name = name

    def build_db_session(self, db_name):
        Session = sessionmaker()
        db_connect_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8;compress=true' % \
                            (self.db_user, self.db_password, self.db_ip, self.db_port, db_name)
        # echo参数控制是否打印sql日志
        engine = create_engine(db_connect_string, echo=False, poolclass=NullPool)
        Session.configure(bind=engine, autoflush=False, expire_on_commit=False)
        return Session()

    def get_db_session(self, db_name):
        session = self.build_db_session(db_name)
        return session

    def close(self):
        for session in self.session_pool:
            session.close()


class HostModel(ServerModel):
    type = 'server_host'

    def __init__(self, name):
        super(HostModel, self).__init__(name)

    def load_parameter(self, local_parameters, project_dict):
        self.server_name = local_parameters.server_name
        self.ip = local_parameters.ip
        self.db_ip = local_parameters.db_ip
        self.db_user = local_parameters.db_user
        self.db_password = local_parameters.db_password
        self.db_port = local_parameters.db_port
        self.__build_server_path_dict(project_dict)

    def __build_server_path_dict(self, project_dict):
        server_path_dict = dict()
        for project_item in project_dict:
            if project_item.dict_type == 'Path_dict':
                server_path_dict[project_item.dict_name] = project_item.dict_value
        self.server_path_dict = server_path_dict


class LocalServerModel(ServerModel):
    type = 'local_server'

    def __init__(self, name):
        super(LocalServerModel, self).__init__(name)

    def load_parameter(self, server_item):
        self.ip = server_item.ip
        self.port = server_item.port
        self.user = server_item.user
        self.pwd = server_item.pwd
        self.db_ip = server_item.db_ip
        self.db_user = server_item.db_user
        self.db_password = server_item.db_password
        self.db_port = server_item.db_port
        self.connect_address = server_item.connect_address
        self.anaconda_home_path = server_item.anaconda_home_path
        self.group_list = server_item.group_list

        self.__ftp_model = SftpModel(self.name, self.ip, self.port, self.user, self.pwd)

    def run_cmd_str(self, cmd_str):
        from cfg import custom_log
        custom_log.log_info_cmd('Server[%s],Cmd[%s] Start.' % (self.name, cmd_str))
        cmd_result_list = []
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ip, self.port, self.user, self.pwd, timeout=90)
            stdin, stdout, stderr = ssh.exec_command(cmd_str)

            error_message_list = []
            cmd_result = stderr.read()
            for item in cmd_result.splitlines():
                error_message_list.append(item)
            if len(error_message_list) > 0:
                error_message_str = '\n'.join(error_message_list)
                custom_log.log_debug_cmd('-----------[Server:%s,stderr]-----------' % self.name)
                if 'Traceback' in error_message_str:
                    error_message = 'Server[%s],Cmd[%s] Error!Error_message:%s' % \
                                    (self.name, cmd_str, error_message_str)
                    custom_log.log_error_cmd(error_message)
                    raise Exception(error_message)
            cmd_result = stdout.read()
            for item in cmd_result.splitlines():
                cmd_result_list.append(item)
            if len(cmd_result_list) > 0:
                custom_log.log_debug_cmd(
                    '-----------[Server:%s,stdout]-----------' % self.name + '\n' + '\n'.join(cmd_result_list))
            ssh.close()
        except Exception:
            error_msg = traceback.format_exc()
            error_message = 'Server[%s],Cmd[%s] Exception!Exception_message:%s' % (self.name, cmd_str, error_msg)
            custom_log.log_error_cmd(error_message)
            raise Exception(error_message)
        custom_log.log_info_cmd('Server[%s],Cmd[%s] Stop!' % (self.name, cmd_str))
        return '\n'.join(cmd_result_list)

    def run_cmd_str2(self, cmd_str):
        from cfg import custom_log
        custom_log.log_info_cmd('Server[%s],Cmd[%s] Start.' % (self.name, cmd_str))
        cmd_result_list = []
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ip, self.port, self.user, self.pwd, timeout=30)
            stdin, stdout, stderr = ssh.exec_command(cmd_str)

            error_message_list = []
            cmd_result = stderr.readlines()
            for item in cmd_result:
                error_message_list.append(item)

            if len(error_message_list) > 0:
                error_message_str = '\n'.join(error_message_list)
                custom_log.log_debug_cmd(
                    '-----------[Server:%s,stderr]-----------' % self.name + '\n' + error_message_str)
                if 'Traceback' in error_message_str:
                    error_message = 'Server[%s],Cmd[%s] Error2!Error_message:%s' % \
                                    (self.name, cmd_str, error_message_str)
                    custom_log.log_error_cmd(error_message)
                    raise Exception(error_message)
            cmd_result = stdout.read()
            for item in cmd_result.splitlines():
                cmd_result_list.append(item)
            if len(cmd_result_list) > 0:
                custom_log.log_debug_cmd(
                    '-----------[Server:%s,stdout]-----------' % self.name + '\n' + '\n'.join(cmd_result_list))
            ssh.close()
        except Exception:
            error_msg = traceback.format_exc()
            error_message = 'Server[%s],Cmd[%s] Exception2!Error_msg:%s' % (self.name, cmd_str, error_msg)
            custom_log.log_error_cmd(error_message)
            raise Exception(error_message)
        custom_log.log_info_cmd('Server[%s],Cmd[%s] Stop!' % (self.name, cmd_str))
        return cmd_result_list

    def is_exist(self, path):
        return self.__ftp_model.is_exist(path)

    def get_size(self, path):
        return self.__ftp_model.get_size(path)

    def list_dir(self, path):
        """
           获取远程目录下所有文件
        """
        return self.__ftp_model.listdir(path)

    def download_file(self, source_file_path, target_file_path):
        """
           下载文件remote_path至dest_path
        """
        return self.__ftp_model.download_file(source_file_path, target_file_path)

    def upload_file(self, source_file_path, target_file_path):
        """
           上传文件
        """
        return self.__ftp_model.upload_file(source_file_path, target_file_path)

    def read_file(self,source_file_path):
        """
            读取文件
        """
        return self.__ftp_model.read_file(source_file_path)

    def check_connect(self):
        """
            检查VPN是否可连接
        """
        check_flag = False
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            sk.connect((self.ip, self.port))
            print 'Server %s:%d Connect OK!' % (self.ip, self.port)
            check_flag = True
        except Exception:
            print '[Error]Server %s:%d Connect Fail!' % (self.ip, self.port)
        finally:
            sk.close()
        return check_flag


class TradeServerModel(LocalServerModel):
    type = 'trade_server'

    def __init__(self, name):
        super(TradeServerModel, self).__init__(name)

    def load_parameter(self, server_item):
        self.server_item = server_item

        self.ip = server_item.ip
        self.port = server_item.port
        self.user = server_item.user
        self.pwd = server_item.pwd
        self.db_ip = server_item.db_ip
        self.db_user = server_item.db_user
        self.db_password = server_item.db_password
        self.db_port = server_item.db_port

        self.connect_address = server_item.connect_address
        self.check_port_list = server_item.check_port_list
        self.etf_base_folder = server_item.etf_base_folder
        self.data_source_type = server_item.data_source_type
        self.market_source_type = server_item.market_source_type
        self.market_file_template = server_item.market_file_template
        self.market_file_localpath = server_item.market_file_localpath
        self.strategy_group_list = server_item.strategy_group_list
        self.is_trade_stock = server_item.is_trade_stock
        self.is_trade_future = server_item.is_trade_future
        self.is_night_session = server_item.is_night_session
        self.is_cta_server = server_item.is_cta_server
        self.is_calendar_server = server_item.is_calendar_server
        self.is_oma_server = server_item.is_oma_server
        self.download_market_file_flag = server_item.download_market_file_flag
        self.server_parameter = server_item.server_parameter
        self.path_parameter = server_item.path_parameter

        self.__build_path_dict()
        self.__ftp_model = SftpModel(self.name, self.ip, self.port, self.user, self.pwd)

    def __build_path_dict(self):
        server_path_dict = dict()
        for path_item in self.path_parameter.split('\n'):
            (path_key, path_value) = path_item.split('=')
            server_path_dict[path_key] = path_value
        server_path_dict['db_backup_folder'] = '%s/db_backup' % server_path_dict['home_folder']
        server_path_dict['mktdtctr_check_folder'] = '%s/dailyjob/MktdtCtr/mkt_files' % server_path_dict['home_folder']
        server_path_dict['tradeplat_log_folder'] = '%s/log' % server_path_dict['tradeplat_project_folder']
        server_path_dict['server_python_folder'] = '%s/server_python' % server_path_dict['eod_project_folder']
        server_path_dict['datafetcher_messagefile'] = '%s/messageFile' % server_path_dict['datafetcher_project_folder']
        server_path_dict['datafetcher_messagefile_backup'] = '%s/messageFile_backup' % server_path_dict[
            'datafetcher_project_folder']
        server_path_dict['datafetcher_marketfile'] = '%s/marketFile' % server_path_dict['datafetcher_project_folder']
        server_path_dict['mktdtctr_data_folder'] = '%s/data' % server_path_dict['mktdtctr_project_folder']

        self.server_path_dict = server_path_dict

    def __reset_parameter(self):
        self.reserve_flag = False
        if hasattr(self, 'server_item') and \
                not (self.server_item.ip_reserve is None or self.server_item.ip_reserve == ''):
            self.ip = self.server_item.ip
            self.port = self.server_item.port
            self.db_ip = self.server_item.db_ip
            self.db_port = self.server_item.db_port

            # 如果无法连接则切换至备用线路
            if not super(TradeServerModel, self).check_connect():
                self.reserve_flag = True
                self.ip = self.server_item.ip_reserve
                self.port = self.server_item.port_reserve
                self.db_ip = self.server_item.db_ip_reserve
                self.db_port = self.server_item.db_port_reserve
                from cfg import custom_log
                custom_log.log_error_cmd('Server[%s] Change To Reserve IP[%s]' % (self.name, self.ip))
        self.__ftp_model = SftpModel(self.name, self.ip, self.port, self.user, self.pwd)

    def build_db_session(self, db_name):
        self.__reset_parameter()
        return super(TradeServerModel, self).build_db_session(db_name)

    def run_cmd_str(self, cmd_str):
        self.__reset_parameter()
        return super(TradeServerModel, self).run_cmd_str(cmd_str)

    def run_cmd_str2(self, cmd_str):
        self.__reset_parameter()
        return super(TradeServerModel, self).run_cmd_str2(cmd_str)

    def is_exist(self, path):
        self.__reset_parameter()
        return self.__ftp_model.is_exist(path)

    def get_size(self, path):
        self.__reset_parameter()
        return self.__ftp_model.get_size(path)

    def list_dir(self, path):
        self.__reset_parameter()
        return self.__ftp_model.list_dir(path)

    def download_file(self, source_file_path, target_file_path):
        self.__reset_parameter()
        return self.__ftp_model.download_file(source_file_path, target_file_path)

    def upload_file(self, source_file_path, target_file_path):
        self.__reset_parameter()
        return self.__ftp_model.upload_file(source_file_path, target_file_path)

    def read_file(self, source_file_path):
        self.__reset_parameter()
        return self.__ftp_model.read_file(source_file_path)

class DepositServerModel(ServerModel):
    type = 'deposit_server'

    def __init__(self, name):
        super(DepositServerModel, self).__init__(name)

    def load_parameter(self, server_item):
        self.ip = server_item.ip
        self.db_ip = server_item.db_ip
        self.db_user = server_item.db_user
        self.db_password = server_item.db_password
        self.db_port = server_item.db_port

        self.connect_address = server_item.connect_address

        self.ftp_type = server_item.ftp_type
        self.ftp_user = server_item.ftp_user
        self.ftp_password = server_item.ftp_password
        self.ftp_wsdl_address = server_item.ftp_wsdl_address
        self.ftp_upload_folder = server_item.ftp_upload_folder
        self.ftp_download_folder = server_item.ftp_download_folder
        if self.ftp_type == 'sftp':
            self.__ftp_model = SftpModel(self.name, self.ip, 22, self.ftp_user, self.ftp_password)
        elif self.ftp_type == 'ftp':
            self.__ftp_model = FtpModel(self.name, self.ip, 21, self.ftp_user, self.ftp_password)
        elif self.ftp_type == 'wsdlftp':
            self.__ftp_model = WsdlFtpModel(self.name, self.ftp_wsdl_address, self.ftp_user, self.ftp_password)

        self.is_trade_stock = server_item.is_trade_stock
        self.is_cta_server = server_item.is_cta_server
        self.is_ftp_monitor = server_item.is_ftp_monitor
        self.strategy_group_list = server_item.strategy_group_list

    def is_exist(self, path):
        return self.__ftp_model.is_exist(path)

    def get_size(self, path):
        return self.__ftp_model.get_size(path)

    def mkdir(self, path):
        return self.__ftp_model.mkdir(path)

    def listdir(self, path):
        return self.__ftp_model.listdir(path)

    def download_file(self, source_file_path, target_file_path):
        return self.__ftp_model.download_file(source_file_path, target_file_path)

    def upload_file(self, source_file_path, target_file_path):
        return self.__ftp_model.upload_file(source_file_path, target_file_path)

    def clear(self, path):
        return self.__ftp_model.remove(path)

    def check_connect(self):
        """
            检查VPN是否可连接
        """
        check_flag = False
        from cfg import custom_log
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        port = 8088
        try:
            sk.connect((self.ip, port))
            custom_log.log_info_cmd('Server %s:%d Connect OK!' % (self.ip, port))
            check_flag = True
        except Exception:
            from cfg import custom_log
            custom_log.log_info_cmd('[Error]Server %s:%d Connect Fail!' % (self.ip, port))
        finally:
            sk.close()
        return check_flag
