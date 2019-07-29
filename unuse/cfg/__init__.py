# -*- coding: utf-8 -*-
import os
import logging.config


def get_config_path():
    folder = os.path.dirname(os.path.abspath(__file__))
    log_config_path = os.path.join(folder, 'logging.conf')
    return log_config_path


log_config_path = get_config_path()
logging.config.fileConfig(log_config_path)
root_logger = logging.getLogger('root')
scheduler_logger = logging.getLogger('scheduler')
cmd_logger = logging.getLogger('cmd')
task_logger = logging.getLogger('task')
