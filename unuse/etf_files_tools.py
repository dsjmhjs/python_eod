# -*- coding: utf-8 -*-
import commands
from eod_aps.server_python import *


def get_index_file(start_date, end_date):
    etf_file_folder = '/home/trader/etf'
    target_file_folder_base = '/home/trader/index_files'
    trading_day_list = date_utils.get_trading_day_list(date_utils.string_toDatetime(start_date),
                                                       date_utils.string_toDatetime(end_date))
    for trading_day in trading_day_list:
        date_str = trading_day.strftime("%Y%m%d")
        umount_command = 'umount /home/trader/etf'
        (status, output) = commands.getstatusoutput(umount_command)
        print output
        mount_command = 'mount -o username=samba,password=smbshare //10.200.66.1/samba/%s %s' % (date_str, etf_file_folder)
        (status, output) = commands.getstatusoutput(mount_command)
        print output
        target_file_folder = '%s/%s' % (target_file_folder_base, date_str)
        copy_command = 'mkdir %s;cd %s;sudo cp * %s' % (target_file_folder, etf_file_folder, target_file_folder)
        print copy_command
        commands.getstatusoutput(copy_command)

if __name__ == '__main__':
    start_date = '2017-04-01'
    end_date = '2017-04-27'
    get_index_file(start_date, end_date)