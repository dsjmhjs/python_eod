# coding: utf-8
import paramiko
from SimpleXMLRPCServer import SimpleXMLRPCServer
from scp import SCPClient


class ScpWsdlService(object):
    """
        ScpWsdlService
    """
    def __init__(self):
        pass

    ip = '172.16.12.118'
    port = 22
    user_name = 'trader'
    password = '123@trader'

    def __ssh_login(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ip, self.port, self.user_name, self.password)
        return ssh

    def is_exist(self, path):
        pass

    def mkdir(self, pathname):
        pass

    def listdir(self, source_folder_path):
        pass

    def download_file(self, source_file_path, target_file_path):
        download_flag = False
        try:
            ssh = self.__ssh_login()
            scp_client = SCPClient(ssh.get_transport(), socket_timeout=15.0)
            scp_client.get(source_file_path, target_file_path)
            ssh.close()
            download_flag = True
        except Exception, e:
            print e
        return download_flag

    def upload_file(self, source_file_path, target_file_path):
        upload_flag = False
        try:
            ssh = self.__ssh_login()
            scp_client = SCPClient(ssh.get_transport(), socket_timeout=15.0)
            scp_client.put(source_file_path, target_file_path)
            ssh.close()
            upload_flag = True
        except Exception, e:
            print e
        return upload_flag


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.11.127', 7088))
    scp_service = ScpWsdlService()
    s.register_instance(scp_service)
    s.serve_forever()

    # scp_server = ServerProxy('http://172.16.11.127:7088')
    # upload_flag = scp_server.upload_file('Z:/dailyjob/ts_order_106.2017-11-28.151118.txt', '/home/trader')
    # print upload_flag
