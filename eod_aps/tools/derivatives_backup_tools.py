# -*- coding: utf-8 -*-
import os

source_path_list = ['Z:/yansheng/Operation',
                    'Z:/yansheng/teamAdmin',
                    ]
target_path = 'Z:/yansheng/BackUp'
password_str = '123456'


def backup_job():
    for source_path in source_path_list:
        backup_file_name = '%s.7z' % os.path.basename(source_path)
        backup_file_path = '%s/%s' % (target_path, backup_file_name)
        zip_cmd = '7z a -t7z %s -p%s %s' % (backup_file_path, password_str, source_path)
        sub = os.system(zip_cmd)
        print sub


if __name__ == "__main__":
    backup_job()