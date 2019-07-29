#!/usr/bin/python
# -*- coding: utf-8 -*-
import mmap
from xmlrpclib import ServerProxy
from eod_aps.model.jsonmmap import ObjectMmap


class JsonMMapTools:
    def reload_eod_data(self):
        json_mmap_server = ServerProxy('http://172.16.11.42:9999')
        json_mmap_server.reload_eod_data()

    def query_eod_config_dict(self):
        eod_config_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='eod_config_mm')
        return eod_config_mm.jsonread_follower()

    def query_email_dict(self):
        email_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='email_mm')
        return email_mm.jsonread_follower()

    def query_server_dict(self):
        server_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='server_mm')
        return server_mm.jsonread_follower()

    def query_server_group_dict(self):
        server_group_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='server_group_mm')
        return server_group_mm.jsonread_follower()

    def query_server_account_dict(self):
        server_account_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='server_account_mm')
        return server_account_mm.jsonread_follower()

    def query_server_pf_account_dict(self):
        server_pf_account_mm = ObjectMmap(-1, 1024 * 1024, access=mmap.ACCESS_READ, tagname='server_pf_account_mm')
        return server_pf_account_mm.jsonread_follower()

    def query_stock_basic_data_dict(self):
        stock_basic_data_mm = ObjectMmap(-1, 1024 * 1024 * 5, access=mmap.ACCESS_READ, tagname='stock_basic_data_mm')
        return stock_basic_data_mm.jsonread_follower()


if __name__ == '__main__':
    jsonmmap_tools = JsonMMapTools()
    print jsonmmap_tools.query_eod_config_dict()
    print jsonmmap_tools.query_email_dict()
    print jsonmmap_tools.query_server_dict()
    print jsonmmap_tools.query_server_group_dict()