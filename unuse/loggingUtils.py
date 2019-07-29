# -*- coding: utf-8 -*-

import sys, traceback
import logging
import os
from logging.handlers import TimedRotatingFileHandler

class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance


class loggingUtils(Singleton):
    def __init__(self, logfile):
        self.logger = logging.getLogger(logfile)

        if not self.logger.handlers:
            self.hdlr = TimedRotatingFileHandler('D:/code/pythoneod/log/%s.log' % logfile,
                                                 when="d",
                                                 interval=1,
                                                 backupCount=7)

            formatter = logging.Formatter('%(levelname)s %(asctime)s %(filename)s[line:%(lineno)d] %(message)s',
                                          '%Y-%m-%d %H:%M:%S')
            self.hdlr.setFormatter(formatter)
            self.logger.addHandler(self.hdlr)
            self.logger.setLevel(logging.INFO)

            # 定义一个Handler打印INFO及以上级别的日志到sys.stderr
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            # 设置日志打印格式
            formatter = logging.Formatter('%(levelname)s %(asctime)s %(filename)s[line:%(lineno)d] %(message)s',
                                          '%Y-%m-%d %H:%M:%S')
            console.setFormatter(formatter)
            # 将定义好的console日志handler添加到root logger
            self.logger.addHandler(console)

    def info(self, msg):
        self.logger.info(msg)
        self.hdlr.flush()

    def debug(self, msg):
        self.logger.debug(msg)
        self.hdlr.flush()

    def error(self, msg):
        self.logger.error(msg)
        self.hdlr.flush()

    def exception(self, msg):
        self.logger.exception(msg)
        self.hdlr.flush()

    def error_sys(self, limit=None):
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        if limit is None:
            if hasattr(sys, 'tracebacklimit'):
                limit = sys.tracebacklimit
        n = 0
        eline = 'n'
        while exceptionTraceback is not None and (limit is None or n < limit):
            f = exceptionTraceback.tb_frame
            lineno = exceptionTraceback.tb_lineno
            co = f.f_code
            filename = co.co_filename
            name = co.co_name
            eline += ' File "%s", line %d, in %s n ' % (filename, lineno, name)
            exceptionTraceback = exceptionTraceback.tb_next
            n = n + 1

        eline += "n".join(traceback.format_exception_only(exceptionType, exceptionValue))
        self.logger.error(eline)
        self.hdlr.flush() 
