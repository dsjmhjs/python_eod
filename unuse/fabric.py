#!/usr/bin/env python
# encoding: utf-8

from fabric.api import *
from fabric.colors import *
import platform
import socket
import os

#操作一致的服务器可以放在一组，同一组的执行同一套操作
env.roledefs = {
            'testserver': ['trader@172.16.12.118:22',],
            'realserver': ['trader@172.16.10.195:22', ]
            }

env.passwords = {
    'trader@172.16.12.118:22': 'admin@yansheng123',
    'trader@172.16.10.195:22': 'admin@yansheng123',
}

@roles('testserver')
def task1():
    run('ls -l | wc -l')
    print green('success')

@roles('realserver')
def task2():
    run('ls ~/temp/ | wc -l')


@hosts('trader@172.16.12.118:22', 'trader@172.16.10.195:22')
@parallel
def remote_task():
    with cd('/var/logs'):
        run('ls -l')

@runs_once   #查看本地系统信息，当有多台主机时只运行一次
def local_task():   #本地任务函数
    local('uname -a')


def dotask():
    execute(task1)
    execute(task2)


def get_ip():
    if platform.system() == 'Windows':
        ip = socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    else:
        ip = os.popen(
            "/sbin/ifconfig |/bin/grep -A1 eth0|/bin/grep 'inet addr'|/bin/awk -F: '{print $2}'|/bin/awk '{print $1}'").read().rstrip(
            '\n')
    print ip


if __name__ == '__main__':
    get_ip()