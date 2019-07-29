# -*- coding: utf-8 -*-
# 每日mount华宝的ETF文件夹
import commands
from eod_aps.server_python import *


today_str = date_utils.get_today_str('%Y%m%d')

print 'start Mount ETF Files'

sudo_command = 'umount %s' % ETF_MOUNT_FOLDER
(status, output) = commands.getstatusoutput(sudo_command)
print 'umount result:%d  output:%s\n' % (status, output)
if (status == 0) or 'not mounted' in output:
    sudo_command = 'mount -o username=samba,password=smbshare //10.200.66.1/samba/HQ/%s %s' % \
                   (today_str, ETF_MOUNT_FOLDER)
    print sudo_command
    (status, output) = commands.getstatusoutput(sudo_command)
    print 'mount result:%d output:%s' % (status, output)
    if status != 0:
        (status, output) = commands.getstatusoutput(sudo_command)
        print 'mount2 result:%d output:%s' % (status, output)