# -*- coding: utf-8 -*-
import os
import inspect
import logging
import logging.config
from colorama import init, Fore, Back, Style


class CustomLog(object):
    """
        日志工具类
    """
    def __init__(self):
        log_config_path = self.__get_config_path()
        logging.config.fileConfig(log_config_path)
        self.__root_logger = logging.getLogger('root')
        self.__task_logger = logging.getLogger('task')
        self.__job_logger = logging.getLogger('job')
        self.__cmd_logger = logging.getLogger('cmd')
        # # 初始化，并且设置颜色设置自动恢复
        # init(autoreset=True)

    def __get_config_path(self):
        folder = os.path.dirname(os.path.abspath(__file__))
        log_config_path = os.path.join(folder, 'logging.conf')
        return log_config_path

    def get_logger(self, log_name):
        if log_name == 'root':
            return self.__root_logger

    # ================root======================================
    def log_error_root(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__root_logger.error(base_stack_info + Back.BLUE + Fore.RED + str(msg) + Style.RESET_ALL)

    def log_info_root(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__root_logger.info(base_stack_info + Fore.YELLOW + str(msg) + Style.RESET_ALL)

    def log_debug_root(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__root_logger.debug(base_stack_info + Fore.BLACK + str(msg) + Style.RESET_ALL)

    # ================task======================================
    def log_error_task(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__task_logger.error(base_stack_info + Back.BLUE + Fore.RED + str(msg) + Style.RESET_ALL)

    def log_info_task(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__task_logger.info(base_stack_info + str(msg))

    def log_debug_task(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__task_logger.debug(base_stack_info + Fore.BLACK + str(msg) + Style.RESET_ALL)

    # ================job======================================
    def log_error_job(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__job_logger.error(base_stack_info + Back.BLUE + Fore.RED + str(msg) + Style.RESET_ALL)

    def log_info_job(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__job_logger.info(base_stack_info + str(msg))

    def log_debug_job(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__job_logger.debug(base_stack_info + Fore.BLACK + str(msg) + Style.RESET_ALL)

    # ================cmd======================================
    def log_error_cmd(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__cmd_logger.error(base_stack_info + Back.BLUE + Fore.RED + str(msg) + Style.RESET_ALL)

    def log_info_cmd(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__cmd_logger.info(base_stack_info + Fore.BLUE + str(msg) + Style.RESET_ALL)

    def log_debug_cmd(self, msg):
        stack = inspect.stack()
        file_name = os.path.basename(stack[1][0].f_code.co_filename)
        line_num = stack[1][0].f_lineno
        base_stack_info = '[%s:%s] ' % (file_name, line_num)
        self.__cmd_logger.debug(base_stack_info + Fore.BLACK + str(msg) + Style.RESET_ALL)


