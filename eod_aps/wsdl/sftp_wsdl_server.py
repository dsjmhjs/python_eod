# coding: utf-8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import sys
import paramiko
import ConfigParser


class SftpWsdlService(object):
    """
        SftpWsdlService
    """
    ftp_ip = ''
    ftp_port = 0
    ftp_userName = ''
    ftp_passWord = ''

    def __init__(self, ftp_ip, ftp_port, user, pwd):
        self.ftp_ip = ftp_ip
        self.ftp_port = int(ftp_port)
        self.ftp_userName = user
        self.ftp_passWord = pwd

    def is_exist(self, path):
        t = None
        is_exist_flag = False
        try:
            t = paramiko.Transport((self.ftp_ip, self.ftp_port))
            t.connect(username=self.ftp_userName, password=self.ftp_passWord)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.stat(path)
            is_exist_flag = True
        except IOError, e:
            print 'Not Exist Path:%s' % path
        finally:
            if t is not None:
                t.close()
        return is_exist_flag

    def mkdir(self, pathname):
        t = None
        is_exist_flag = False
        try:
            t = paramiko.Transport((self.ftp_ip, self.ftp_port))
            t.connect(username=self.ftp_userName, password=self.ftp_passWord)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.mkdir(pathname, 0755)
            is_exist_flag = True
        except IOError, e:
            print 'Make Path:%s Fail!' % pathname
        finally:
            if t is not None:
                t.close()
        return is_exist_flag

    def listdir(self, source_folder_path):
        return_list = []
        try:
            t = paramiko.Transport((self.ftp_ip, self.ftp_port))
            t.connect(username=self.ftp_userName, password=self.ftp_passWord)
            sftp = paramiko.SFTPClient.from_transport(t)
            return_list = sftp.listdir(source_folder_path)
        except IOError, e:
            print 'Miss Path:%s Fail!' % source_folder_path
        finally:
            if t is not None:
                t.close()
        return return_list

    def download_file(self, source_file_path, target_file_path):
        download_flag = False
        try:
            t = paramiko.Transport((self.ftp_ip, self.ftp_port))
            t.connect(username=self.ftp_userName, password=self.ftp_passWord)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.get(source_file_path, target_file_path)
            download_flag = True
        except IOError, e:
            print 'Download File:%s Fail!' % source_file_path
        finally:
            if t is not None:
                t.close()
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        upload_flag = False
        try:
            t = paramiko.Transport((self.ftp_ip, self.ftp_port))
            t.connect(username=self.ftp_userName, password=self.ftp_passWord)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.put(source_file_path, target_file_path)
            upload_flag = True
        except IOError, e:
            print 'Upload File:%s Fail!' % source_file_path
        finally:
            if t is not None:
                t.close()
        return upload_flag

    def remove(self, path):
        rmdir_flag = False
        t = paramiko.Transport((self.ftp_ip, self.ftp_port))
        t.connect(username=self.ftp_userName, password=self.ftp_passWord)
        sftp = paramiko.SFTPClient.from_transport(t)
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
    sftp_wsdl_service = SftpWsdlService(ftp_ip, ftp_port, user, pwd)
    s.register_instance(sftp_wsdl_service)
    s.serve_forever()
