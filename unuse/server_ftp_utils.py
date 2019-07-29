# -*- coding: utf-8 -*-
from ftplib import FTP, error_perm
from eod_aps.model.server_constans import ServerConstant


class FtpUtils:
    server_name = None
    ftp = None

    def __init__(self, server_name):
        self.server_name = server_name

    def __enter__(self):
        server_model = ServerConstant().get_server_model(self.server_name)
        self.ftp = FTP()
        self.ftp.set_pasv(False)
        self.ftp.connect(server_model.ip, server_model.port)
        self.ftp.login(server_model.userName, server_model.passWord)
        return self

    def is_exist(self, path):
        is_exist_flag = False
        try:
            self.ftp.cwd(path)
            is_exist_flag = True
        except error_perm:
            print path, "not exist"
        return is_exist_flag

    def mkdir(self, pathname):
        self.ftp.mkd(pathname)

    def listdir(self, source_folder_path):
        self.ftp.cwd(source_folder_path)
        files = self.ftp.nlst()
        return list(reversed(sorted(files)))

    def download_file(self, source_file_path, target_file_path):
        file_handler = open(target_file_path, 'wb')
        self.ftp.retrbinary("RETR %s" % source_file_path, file_handler.write)
        file_handler.close()
        print 'Download file:%s from:%s success' % (source_file_path, self.server_name)
        return True

    def upload_file(self, source_file_path, target_file_path):
        try:
            bufsize = 1024
            fp = open(source_file_path, 'rb')
            self.ftp.storbinary('STOR ' + target_file_path, fp, bufsize)
            print 'Upload file:%s, to:%s success' % (source_file_path, self.server_name)
            self.ftp.set_debuglevel(0)
            fp.close()
        except Exception, e:
            print e
            return False
        return True

    def __exit__(self, type, value, traceback):
        self.ftp.quit()

if __name__ == '__main__':
    ftp_utils = FtpUtils('huabao')