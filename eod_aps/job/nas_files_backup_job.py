# -*- coding: utf-8 -*-
# Nas文件备份目录
import os
import traceback
import shutil
from eod_aps.job import *


def nas_files_backup_job():
    nas_backup_parameter_str = const.EOD_CONFIG_DICT['nas_backup_parameter']

    source_path, target_path = nas_backup_parameter_str.split('|')
    try:
        if os.path.exists(target_path):
            shutil.rmtree(target_path, True)
        shutil.copytree(source_path, target_path)
    except Exception:
        error_msg = traceback.format_exc()
        print error_msg


if __name__ == '__main__':
    nas_files_backup_job()
