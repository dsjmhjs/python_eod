import os
from eod_aps.check import *
from eod_aps.job import *
from eod_aps.model.server_constans import server_constant


def download_depositplat_log_check(job_name):
    deposit_servers_list = server_constant.get_deposit_servers()
    download_day_str = date_utils.get_today_str()

    for server_name in deposit_servers_list:
        server_model = server_constant.get_server_model(server_name)
        ftp_server = server_model
        source_folder_path = server_model.ftp_download_folder
        source_date_folder_path = '%s/%s' % (source_folder_path, download_day_str)
        log_save_path = LOG_BACKUP_FOLDER_TEMPLATE % server_name

        check_flag = False
        for file_name in os.listdir(log_save_path):
            if 'tradeplat_log_%s' % download_day_str in file_name:
                source_file_path = '%s/%s' % (source_date_folder_path, file_name)
                check_file_path = '%s/%s' % (log_save_path, file_name)
                if os.path.exists(log_save_path):
                    if os.path.exists(check_file_path):
                        if long(ftp_server.get_size(source_file_path)) == long(os.stat(check_file_path).st_size):
                            check_flag = True
                            break
                email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                                  'Download Error.File:%s Download Fail!' % check_file_path)
        if not check_flag:
            email_utils1.send_email_group_all('[ERROR]After Check_Job:%s' % job_name,
                                              'Download Error.Log File Missing!')


if __name__ == '__main__':
    download_depositplat_log_check('check')
