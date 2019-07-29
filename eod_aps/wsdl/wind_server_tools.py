# -*- coding: utf-8 -*-
from WindPy import *
from SimpleXMLRPCServer import SimpleXMLRPCServer

w_server = w()


def w_close():
    w.close()
    return 0


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.12.99', 8080), allow_none=True)
    s.register_instance(w_server)
    s.register_function(w_close)
    s.serve_forever()
