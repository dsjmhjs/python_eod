# -*- coding: utf-8 -*-
import subprocess
from SimpleXMLRPCServer import SimpleXMLRPCServer


def guoxin_net_test():
    result_str = subprocess.check_output('tracert 192.168.14.67')
    ip_line = result_str.split('\n')[3]
    ip_str = ip_line[-15:-1].replace(' ', '').replace('m', '').replace('s', '')
    if ip_str == '192.168.14.2':
        return True
    else:
        return False


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.10.195', 8888))
    s.register_function(guoxin_net_test)
    s.serve_forever()