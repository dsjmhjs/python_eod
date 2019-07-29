# -*- coding: utf-8 -*-
# 控制aggregator启停
from eod_aps.job import *


aggregator_server_name = 'aggregator'


def start_aggregator_day():
    server_model = server_constant.get_server_model(aggregator_server_name)
    start_cmd = ['cd ~/apps/Aggregator',
                 'rm config.aggregator.txt',
                 'ln -s config.aggregator_day.txt config.aggregator.txt',
                 './start.aggregator.sh'
                 ]
    server_model.run_cmd_str(';'.join(start_cmd))


def start_aggregator_night():
    server_model = server_constant.get_server_model(aggregator_server_name)
    start_cmd = ['cd ~/apps/Aggregator',
                 'rm config.aggregator.txt',
                 'ln -s config.aggregator_night.txt config.aggregator.txt',
                 './start.aggregator.sh'
                 ]
    server_model.run_cmd_str(';'.join(start_cmd))


def stop_aggregator():
    server_model = server_constant.get_server_model(aggregator_server_name)
    stop_cmd = 'screen -r Aggregator -X quit'
    server_model.run_cmd_str(stop_cmd)


def restart_aggregator_day():
    stop_aggregator()
    start_aggregator_day()


def restart_aggregator_night():
    stop_aggregator()
    start_aggregator_night()


def encrypt_password(password_str):
    """
       加密密碼
    """
    server_model = server_constant.get_server_model(aggregator_server_name)
    cmd_list = ['cd ~/apps/Aggregator/encrypt_decrypt',
                './Encrypt %s' % password_str,
                ]
    return server_model.run_cmd_str(';'.join(cmd_list))


def decrypt_password(password_str):
    """
       解密密碼
    """
    server_model = server_constant.get_server_model(aggregator_server_name)
    cmd_list = ['cd ~/apps/Aggregator/encrypt_decrypt',
                './Decrypt %s' % password_str,
                ]
    return server_model.run_cmd_str(';'.join(cmd_list))


if __name__ == '__main__':
    decrypt_password()
