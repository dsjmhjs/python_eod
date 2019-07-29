# -*- coding: utf-8 -*-
from SimpleXMLRPCServer import SimpleXMLRPCServer
import subprocess
import psutil

aggregator_exe_path = r'D:/Aggregator/ATP-Aggregator-V3.1.0'
aggregator_night_exe_path = r'D:/Aggregator/ATP-Aggregator-V3.1.0_night'
aggregator_exe_name = 'TradeMonitor.exe'


def start_aggregator_job():
    subprocess.Popen(aggregator_exe_path + '/' + aggregator_exe_name)
    return 0


# 夜盘aggregator
def start_aggregator_night_job():
    subprocess.Popen(aggregator_night_exe_path + '/' + aggregator_exe_name)
    return 0


# 杀死进程
def __kill_process(process_name):
    pid_list = psutil.pids()
    for each_pid in pid_list:
        try:
            each_pro = psutil.Process(each_pid)
            if each_pro.name().lower() == process_name.lower():
                print("found process")
                print("process_name=%s"%each_pro.name())
                print('process_exe=%s'%each_pro.exe())
                print('process_cwd=%s'%each_pro.cwd())
                print('process_cmdline=%s'%each_pro.cmdline())
                print('process_status=%s'%each_pro.status())
                print('process_username=%s'%each_pro.username())
                print('process_createtime=%s'%each_pro.create_time())
                print('now will kill this process')
                each_pro.terminate()
                each_pro.wait(timeout=3)
        except psutil.NoSuchProcess, pid:
            print "no process found with pid=%s"% pid


def kill_trade_monitor_job():
    print 'kill_process TradeMonitor'
    __kill_process('TradeMonitor.exe')
    return 'haha'


if __name__ == "__main__":
    s = SimpleXMLRPCServer(('172.16.10.188', 8889))
    s.register_function(kill_trade_monitor_job)
    s.register_function(start_aggregator_job)
    s.register_function(start_aggregator_night_job)
    s.serve_forever()

