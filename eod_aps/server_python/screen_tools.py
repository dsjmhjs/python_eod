# -*- coding: utf-8 -*-
import time
from screenutils import Screen
from eod_aps.model.eod_parse_arguments import parse_arguments


def screen_manager(screen_name, command):
    print 'Enter screen_manager,screen_name:%s,command:%s.' % (screen_name, command)
    s = Screen(screen_name)
    if not s.exists:
        print '[Error]Screen:%s is not exists!' % screen_name
        return
    s.enable_logs()
    if command is not None:
        s.send_commands(command)
    time.sleep(2)
    print next(s.logs)
    s.disable_logs()
    print 'Exit screen_manager,screen_name:%s,command:%s.' % (screen_name, command)


if __name__ == '__main__':
    options = parse_arguments()
    screen_name = options.screen_name
    command = options.command
    screen_manager(screen_name, command)
