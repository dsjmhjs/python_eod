# -*- coding: utf-8 -*-
# md5校验
import os
import hashlib
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.email_utils import EmailUtils
from cfg import custom_log

date_utils = DateUtils()
email_utils = EmailUtils(const.EMAIL_DICT['group6'])
interval_time = 60


def __read_chunks(fh):
    fh.seek(0)
    chunk = fh.read(8096)
    while chunk:
        yield chunk
        chunk = fh.read(8096)
    else:
        fh.seek(0)  # 最后要将游标放回文件开头


# 获取文件大小
def get_file_size(local_file_path):
    if os.path.exists(local_file_path):
        size = os.path.getsize(local_file_path)
        size = '%.2f' % (size / float(1024 * 1024 * 1024))
    else:
        size = '0'
    return size


# 计算本地文件的MD5值
def get_local_file_md5(local_file_path):
    m = hashlib.md5()
    if os.path.exists(local_file_path):
        with open(local_file_path, "rb") as fh:
            for chunk in __read_chunks(fh):
                m.update(chunk)
        local_md5_value = m.hexdigest()
    else:
        local_md5_value = ""
    custom_log.log_info_task('Local File:%s,MD5:%s' % (local_file_path, local_md5_value))
    return local_md5_value


# 计算服务器文件的MD5值
def get_server_file_md5(server_model, server_file_path):
    cmd_str = 'md5sum %s' % server_file_path
    run_result = server_model.run_cmd_str(cmd_str)
    
    server_md5_value = run_result.split(' ')[0]
    custom_log.log_info_task('Server File:%s,MD5:%s' % (server_file_path, server_md5_value))
    return server_md5_value


def md5_check_download_file(server_model, server_file_path, local_file_path):
    server_md5_value = get_server_file_md5(server_model, server_file_path)
    local_md5_value = get_local_file_md5(local_file_path)
    if local_md5_value == server_md5_value:
        email_content = 'Download Success!Server_Name:%s,File_Name:%s' % \
                        (server_model.name, server_file_path)
        custom_log.log_info_task(email_content)
        return True
    else:
        email_content = 'Download Error!Server_Name:%s,File_Name:%s,Server MD5:%s, Local MD5:%s' % \
                        (server_model.name, server_file_path, server_md5_value, local_md5_value)
        custom_log.log_error_task(email_content)
        email_utils.send_email_group_all('[ERROR]%s:文件下载失败!' % server_model.name, email_content)
        return False


def file_md5_check(server_model, server_file_path, local_file_path):
    server_md5_value = get_server_file_md5(server_model, server_file_path)
    local_md5_value = get_local_file_md5(local_file_path)
    if local_md5_value == server_md5_value:
        return True
    else:
        return False


# 校验tar压缩文件是否成功，即可正常解压缩
def check_tar_file(server_model, file_base_path, tar_file_name):
    cmd_list = ['cd %s' % file_base_path,
                'tar -tf %s' % tar_file_name
    ]
    run_result = server_model.run_cmd_str(';'.join(cmd_list))
    if 'Unexpected' in run_result:
        return False
    return True


def md5_str(input_str):
    m = hashlib.md5()
    m.update(str(input_str))
    return m.hexdigest()


if __name__ == '__main__':
    pass
