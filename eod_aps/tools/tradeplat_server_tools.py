# -*- coding: utf-8 -*-
from eod_aps.tools.tradeplat_message_tools import *


def server_is_panic(server_name):
    socket = socket_init(server_name)
    serverinfo_msg = send_serverinfo_request_msg(socket)
    return serverinfo_msg.IsPanic


if __name__ == '__main__':
    print server_is_panic('guoxin') == False