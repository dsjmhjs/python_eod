# -*- coding: utf-8 -*-
# 文件上传工具
import os
import tarfile
import threading
import traceback

from eod_aps.job import *


ysquant_job_cmd_dict = {
    'index_minute': '~/projects/ysquant_sharry_update/stock/index_minute.py -m sharry',
    'minute_sharry_writer': '~/projects/ysquant_sharry_update/stock/minute_sharry_writer.py',
    'daily_sharry_writer1': '~/projects/ysquant_sharry_update/stock/daily_sharry_writer.py -a ashare',
    'daily_sharry_writer2': '~/projects/ysquant_sharry_update/stock/daily_sharry_writer.py',
    'index_bin': '~/projects/ysquant_sharry_update/stock/index_bin.py -m sharry -a ashare',
    'not_daily_reader': '~/projects/ysquant_sharry_update/stock/not_daily_reader.py -m sharry -a ashare',
    'basic_sharry': '~/projects/ysquant_sharry_update/stock/basic_sharry.py -m sharry -a ashare'
}


def __ysquant_cmd_job(server_name, dict_key):
    try:
        base_cmd = ysquant_job_cmd_dict[dict_key]
        server_model = server_constant.get_server_model(server_name)
        job_cmd = '%s %s' % \
                  (server_model.anaconda_home_path, base_cmd)
        custom_log.log_info_job(job_cmd)
        run_result_message = server_model.run_cmd_str(job_cmd)
        if run_result_message is not None and run_result_message.endswith('True'):
            pass
        else:
            email_utils18.send_email_group_all('[Error]__ysquant_cmd_job.Server:%s,dict_key:%s' % (server_name, dict_key),
                                              'Job Run Error:%s' % run_result_message)
    except Exception:
        error_msg = traceback.format_exc()
        custom_log.log_error_job(error_msg)
        email_utils18.send_email_group_all('[Error]__ysquant_cmd_job:%s.dict_key:%s' % (server_name, dict_key),
                                          error_msg)


def ysquant_cmd_job(server_name_tuple, dict_key):
    threads = []
    for server_name in server_name_tuple:
        t = threading.Thread(target=__ysquant_cmd_job, args=(server_name, dict_key))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == '__main__':
    ysquant_cmd_job(('local_178',), 'index_minute')
