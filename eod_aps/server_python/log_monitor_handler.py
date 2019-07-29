#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import pyinotify
import traceback
import subprocess
import requests
import re
import datetime
import json
import time
from eod_aps.server_python import *


class OnWriteHandler(pyinotify.ProcessEvent):
    def __init__(self, monitor_file_path, filter_date_str, ip, port):
        super(OnWriteHandler, self).__init__()
        self.monitor_file_path = monitor_file_path
        self.filter_date_str = filter_date_str
        self.ip = ip
        self.port = port

    def get_err_info(self, error_file_name):
        log_cmd_list = [
            "cd %s" % self.monitor_file_path,
            "grep 'PANIC' %s" % error_file_name,
            "grep 'NORMAL' screenlog_MainFrame*%s*.log" % self.filter_date_str
        ]
        shell_cmd = ";".join(log_cmd_list)
        rst = subprocess.Popen(shell_cmd, shell=True, stdout=subprocess.PIPE)
        return_message_items = rst.stdout.readlines()
        format_message_dict = {}
        msg = {}
        if not return_message_items:
            return msg

        for return_message_item in return_message_items:
            date_str = re.findall(r"(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", return_message_item)[0]
            format_message_dict[date_str] = return_message_item
        max_time = max(format_message_dict.keys())
        check_message = format_message_dict[max_time]
        if 'PANIC' in check_message:
            msg['Title'] = '[PANIC]Server:%s Time:%s' % (lOCAL_SERVER_NAME, max_time)

            cmd_list = ['cd %s' % self.monitor_file_path,
                        'tail -50 %s' % error_file_name
                        ]
            shell_cmd = ";".join(cmd_list)
            rst = subprocess.Popen(shell_cmd, shell=True, stdout=subprocess.PIPE)
            return_message_items = rst.stdout.readlines()
            msg['Content'] = 'FileName:%s\n%s' % (error_file_name, ''.join(return_message_items))
        return msg

    def __notify_manager(self, msg):
        try:
            params = {'message': msg}
            r = requests.post(url='http://%s:%s/system/server_notify' % (self.ip, self.port), json=params)
            print r.text
        except Exception:
            error_msg = traceback.format_exc()
            print error_msg

    def process_IN_MODIFY(self, event):
        if 'error' in event.name and event.name.endswith('.log'):
            msg_dict = self.get_err_info(event.name)
            if msg_dict:
                self.__notify_manager(json.dumps(msg_dict))


def inotify_monitor(ip, port):
    monitor_file_path = '%s/log' % TRADEPLAT_PROJECT_FOLDER

    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE | pyinotify.IN_MODIFY

    filter_date_str = datetime.datetime.now().strftime('%Y%m%d')
    event_handler = OnWriteHandler(monitor_file_path, filter_date_str, ip, port)
    notifier = pyinotify.Notifier(wm, event_handler)
    wm.add_watch(monitor_file_path, mask, rec=True, auto_add=True)
    print 'start to watch path: %s' % monitor_file_path

    now_time = long(date_utils.get_today_str('%H%M%S'))
    while 90000 < now_time < 150000:
        try:
            notifier.process_events()
            if notifier.check_events():
                notifier.read_events()
        except KeyboardInterrupt:
            notifier.stop()
            break
        time.sleep(5)


if __name__ == "__main__":
    inotify_monitor('127.0.0.1', 8887)
