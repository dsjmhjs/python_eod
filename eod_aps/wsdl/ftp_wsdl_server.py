# coding: utf-8
from SimpleXMLRPCServer import SimpleXMLRPCServer
from ftplib import FTP, error_perm
import sys
import socket
import ConfigParser


class FtpWsdlService(object):
    """
        FTP工具类
    """
    __ftp_ip = ''
    __ftp_port = 0
    __ftp_userName = ''
    __ftp_passWord = ''
    buff_size = 40960

    def __init__(self, ftp_ip, ftp_port, user, pwd):
        self.__ftp_ip = ftp_ip
        self.__ftp_port = ftp_port
        self.__ftp_userName = user
        self.__ftp_passWord = pwd

    def __ftp_login(self):
        socket.setdefaulttimeout(120)
        ftp = FTP()
        ftp.set_debuglevel(2)
        ftp.set_pasv(False)
        ftp.connect(self.__ftp_ip, self.__ftp_port)
        ftp.login(self.__ftp_userName, self.__ftp_passWord)
        # print ftp.getwelcome()
        print 'Login Success!'
        return ftp

    def is_exist(self, path):
        is_exist_flag = False
        ftp = self.__ftp_login()
        try:
            ftp.cwd(path)
            is_exist_flag = True
        except error_perm:
            file_size = 0
            try:
                file_size = ftp.size(path)
            except error_perm:
                print 'Not Exist Path:%s' % path

            if file_size > 0:
                is_exist_flag = True
            else:
                print path, "not exist"
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return is_exist_flag

    def get_size(self, path):
        ftp = self.__ftp_login()
        file_size = 0
        try:
            file_size = ftp.size(path)
        except error_perm:
            print 'Not Exist Path:%s' % path
        return str(file_size)

    def mkdir(self, pathname):
        mkdir_flag = False
        ftp = self.__ftp_login()
        try:
            ftp.mkd(pathname)
            mkdir_flag = True
        except error_perm:
            print error_perm
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return mkdir_flag

    def listdir(self, source_folder_path):
        return_list = []
        ftp = self.__ftp_login()
        try:
            ftp.cwd(source_folder_path)
            files = ftp.nlst()
            return_list = list(reversed(sorted(files)))
        except error_perm:
            print error_perm
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return return_list

    def download_file(self, source_file_path, target_file_path):
        download_flag = False
        ftp = self.__ftp_login()
        try:
            with open(target_file_path, 'wb') as file_handler:
                ftp.retrbinary("RETR %s" % source_file_path, file_handler.write, self.buff_size)
            print 'Download file:%s from:%s success' % (source_file_path, self.__ftp_ip)
            download_flag = True
        except error_perm:
            print error_perm
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        upload_flag = False
        ftp = self.__ftp_login()
        try:
            with open(source_file_path, 'rb') as file_handler:
                ftp.storbinary('STOR ' + target_file_path, file_handler, self.buff_size)
            print 'Upload file:%s, to:%s success' % (source_file_path, self.__ftp_ip)
            upload_flag = True
        except error_perm:
            print error_perm
        finally:
            ftp.set_debuglevel(0)
            ftp.quit()
        return upload_flag

    def remove(self, path):
        rmdir_flag = False
        ftp = self.__ftp_login()
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


if __name__ == '__main__':
    config_file_name = str(sys.argv[1])

    cp = ConfigParser.SafeConfigParser()
    cp.read('./%s' % config_file_name)

    wsdl_ip = cp.get('config', 'wsdl_ip')
    wsdl_port = cp.get('config', 'wsdl_port')

    ftp_ip = cp.get('config', 'ftp_ip')
    ftp_port = cp.get('config', 'ftp_port')
    user = cp.get('config', 'user')
    pwd = cp.get('config', 'pwd')

    s = SimpleXMLRPCServer((wsdl_ip, int(wsdl_port)))
    ftp_wsdl_service = FtpWsdlService(ftp_ip, ftp_port, user, pwd)
    s.register_instance(ftp_wsdl_service)
    s.serve_forever()
