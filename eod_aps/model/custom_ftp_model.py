# -*- coding: utf-8 -*-
import socket
import paramiko
from ftplib import FTP, error_perm
from xmlrpclib import ServerProxy


class FtpModel(object):
    """
        FTP工具类
    """

    def __init__(self, name, ftp_ip, ftp_port, user, pwd):
        self.name = name
        self.__ftp_ip = ftp_ip
        self.__ftp_port = ftp_port
        self.__ftp_userName = user
        self.__ftp_passWord = pwd
        self.__buff_size = 40960

    def __login(self):
        socket.setdefaulttimeout(120)
        ftp = FTP()
        ftp.set_debuglevel(2)
        ftp.set_pasv(False)
        ftp.connect(self.__ftp_ip, self.__ftp_port)
        ftp.login(self.__ftp_userName, self.__ftp_passWord)

        from cfg import custom_log
        custom_log.log_info_cmd('Login Success!')
        return ftp

    def is_exist(self, path):
        is_exist_flag = False
        ftp = self.__login()
        try:
            ftp.cwd(path)
            is_exist_flag = True
        except error_perm:
            file_size = 0
            try:
                file_size = ftp.size(path)
            except error_perm:
                pass

            if file_size > 0:
                is_exist_flag = True
            else:
                from cfg import custom_log
                custom_log.log_error_cmd('FTP Not Exist:%s' % path)
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return is_exist_flag

    def get_size(self, path):
        ftp = self.__login()
        file_size = 0
        try:
            file_size = ftp.size(path)
        except error_perm:
            from cfg import custom_log
            custom_log.log_error_cmd('FTP Not Exist:%s' % path)
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return file_size

    def mkdir(self, path):
        mkdir_flag = False
        ftp = self.__login()
        try:
            ftp.mkd(path)
            mkdir_flag = True
        except error_perm:
            from cfg import custom_log
            custom_log.log_error_cmd(error_perm)
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return mkdir_flag

    def listdir(self, path):
        return_list = []
        ftp = self.__login()
        try:
            ftp.cwd(path)
            files = ftp.nlst()
            return_list = list(reversed(sorted(files)))
        except error_perm:
            from cfg import custom_log
            custom_log.log_error_cmd(error_perm)
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return return_list

    def download_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        download_flag = False
        ftp = self.__login()
        try:
            with open(target_file_path, 'wb') as file_handler:
                ftp.retrbinary("RETR %s" % source_file_path, file_handler.write, self.__buff_size)
            custom_log.log_info_cmd('Download File:%s From:%s Success.' % (source_file_path, self.name))
            download_flag = True
        except error_perm:
            custom_log.log_error_cmd(error_perm)
            custom_log.log_error_cmd('[Error]Download File:%s From:%s Fail!' % (source_file_path, self.name))
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        upload_flag = False
        ftp = self.__login()
        try:
            with open(source_file_path, 'rb') as file_handler:
                ftp.storbinary('STOR ' + target_file_path, file_handler, self.__buff_size)
            custom_log.log_info_cmd('Upload File:%s To:%s Success.' % (source_file_path, self.name))
            upload_flag = True
        except error_perm:
            custom_log.log_error_cmd(error_perm)
            custom_log.log_error_cmd('[Error]Upload File:%s To:%s Fail!' % (source_file_path, self.name))
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return upload_flag

    def remove(self, path):
        rmdir_flag = False
        ftp = self.__login()
        try:
            ftp.cwd(path)
            files = ftp.nlst()
            for file_name in files:
                self.remove('%s/%s' % (path, file_name))
            ftp.rmd(path)
            rmdir_flag = True
        except error_perm:
            try:
                ftp.delete(path)
                rmdir_flag = True
            except Exception as e:
                print e
                from cfg import custom_log
                custom_log.log_error_cmd('Clear Path:%s Fail!' % path)
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return rmdir_flag


class SftpModel(object):
    """
        SFTP工具类
    """

    def __init__(self, name, sftp_ip, sftp_port, user, pwd):
        self.name = name
        self.__sftp_ip = sftp_ip
        self.__sftp_port = int(sftp_port)
        self.__sftp_userName = user
        self.__sftp_passWord = pwd

    def __login(self):
        t = paramiko.Transport((self.__sftp_ip, self.__sftp_port))
        t.connect(username=self.__sftp_userName, password=self.__sftp_passWord)
        sftp = paramiko.SFTPClient.from_transport(t)
        return t, sftp

    def is_exist(self, path):
        t, sftp = self.__login()
        is_exist_flag = False
        try:
            sftp.stat(path)
            is_exist_flag = True
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Not Exist Path:%s' % path)
        finally:
            if t is not None:
                t.close()
        return is_exist_flag

    def get_size(self, path):
        t, sftp = self.__login()
        file_size = 0
        try:
            file_size = sftp.stat(path).st_size
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Not Exist Path:%s' % path)
        finally:
            if t is not None:
                t.close()
        return file_size

    def mkdir(self, pathname):
        t, sftp = self.__login()
        is_exist_flag = False
        try:
            sftp.mkdir(pathname, 0755)
            is_exist_flag = True
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Make Path:%s Fail!' % pathname)
        finally:
            if t is not None:
                t.close()
        return is_exist_flag

    def listdir(self, source_folder_path):
        file_list = []
        t, sftp = self.__login()
        try:
            return_array = sftp.listdir(source_folder_path)
            file_list = [x for x in return_array]
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Miss Path:%s Fail!' % source_folder_path)
        finally:
            if t is not None:
                t.close()
        return file_list

    def download_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        download_flag = False
        t, sftp = self.__login()
        try:
            sftp.get(source_file_path, target_file_path)
            download_flag = True
            custom_log.log_info_cmd('Download File:%s From:%s Success.' % (source_file_path, self.name))
        except IOError:
            custom_log.log_error_cmd('[Error]Download File:%s From:%s Fail!' % (source_file_path, self.name))
        finally:
            if t is not None:
                t.close()
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        upload_flag = False
        t, sftp = self.__login()
        try:
            sftp.put(source_file_path, target_file_path)
            upload_flag = True
            custom_log.log_info_cmd('Upload File:%s To:%s Success.' % (source_file_path, self.name))
        except IOError:
            custom_log.log_error_cmd('[Error]Upload File:%s To:%s Fail!' % (source_file_path, self.name))
        finally:
            if t is not None:
                t.close()
        return upload_flag

    def read_file(self, source_file_path):
        t, sftp = self.__login()
        file_size = self.get_size(source_file_path)
        reader = sftp.open(source_file_path, 'rb')
        size = 0
        result = ''
        temp_read_size = 10485760
        while True:
            if file_size - size >= temp_read_size:
                read_size = temp_read_size
            else:
                read_size = file_size - size
            # print size, file_size
            data = reader.read(read_size)
            result += data
            size += len(data)
            if len(data) == 0:
                reader.close()
                print size, 'break'
                break
        return result

    def remove(self, path):
        rmdir_flag = False
        t, sftp = self.__login()
        try:
            try:
                sftp.chdir(path)
                for file_name in sftp.listdir(path):
                    self.remove('%s/%s' % (path, file_name))
                sftp.rmdir(path)
            except:
                sftp.remove(path)
            rmdir_flag = True
        except Exception as e:
            print e
            from cfg import custom_log
            custom_log.log_error_cmd('Clear Path:%s Fail!' % path)
        finally:
            if t is not None:
                t.close()
        return rmdir_flag


class WsdlFtpModel(object):
    """
        Wsdl方式接入FTP工具类
    """

    def __init__(self, name, ftp_wsdl_address, user, pwd):
        self.name = name
        self.__ftp_wsdl_address = ftp_wsdl_address
        self.__sftp_userName = user
        self.__sftp_passWord = pwd

    def __login(self):
        ftp_server = ServerProxy(self.__ftp_wsdl_address)
        return ftp_server

    def is_exist(self, path):
        ftp_server = self.__login()
        is_exist_flag = False
        try:
            is_exist_flag = ftp_server.is_exist(path)
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Not Exist Path:%s' % path)
        return is_exist_flag

    def get_size(self, path):
        ftp_server = self.__login()
        file_size = 0
        try:
            file_size = ftp_server.get_size(path)
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Not Exist Path:%s' % path)
        return file_size

    def mkdir(self, pathname):
        ftp_server = self.__login()
        is_exist_flag = False
        try:
            ftp_server.mkdir(pathname)
            is_exist_flag = True
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Make Path:%s Fail!' % pathname)
        return is_exist_flag

    def listdir(self, source_folder_path):
        file_list = []
        ftp_server = self.__login()
        try:
            return_array = ftp_server.listdir(source_folder_path)
            file_list = [x for x in return_array]
        except IOError:
            from cfg import custom_log
            custom_log.log_error_cmd('Miss Path:%s Fail!' % source_folder_path)
        return file_list

    def download_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        download_flag = False
        ftp_server = self.__login()
        try:
            ftp_server.download_file(source_file_path, target_file_path)
            download_flag = True
            custom_log.log_info_cmd('Download File:%s From:%s Success.' % (source_file_path, self.name))
        except IOError:
            custom_log.log_error_cmd('[Error]Download File:%s From:%s Fail!' % (source_file_path, self.name))
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        from cfg import custom_log
        upload_flag = False
        ftp_server = self.__login()
        try:
            ftp_server.upload_file(source_file_path, target_file_path)
            upload_flag = True
            custom_log.log_info_cmd('Upload File:%s To:%s Success.' % (source_file_path, self.name))
        except IOError:
            custom_log.log_error_cmd('[Error]Upload File:%s To:%s Fail!' % (source_file_path, self.name))
        return upload_flag

    def remove(self, path):
        rmdir_flag = False
        ftp_server = self.__login()
        try:
            ftp_server.remove(path)
            rmdir_flag = True
        except Exception as e:
            from cfg import custom_log
            custom_log.log_error_cmd('Clear Path:%s Fail!' % path)
        return rmdir_flag
