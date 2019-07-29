# -*- coding: utf-8 -*-
# 对假期的minutebar进行清理
import os
from eod_aps.model.eod_const import const

holiday_list = ['20150105.csv', '20150225.csv', '20150407.csv', '20150504.csv', '20150623.csv', '20150907.csv',
                '20150928.csv', '20151008.csv', '20160104.csv', '20160215.csv', '20160405.csv', '20160503.csv']
night_filter_list = ['21', '22', '23', '00', '01', '02']

LOCAL_MINUTE_BAR_FOLDER = 'H:/data_history/BAR/1min'


def __holiday_minutebar_rebuild(date_file_name):
    for folder_name in os.listdir(LOCAL_MINUTE_BAR_FOLDER):
        minutebar_file_path = '%s/%s/%s' % (LOCAL_MINUTE_BAR_FOLDER, folder_name, date_file_name)
        if not os.path.exists(minutebar_file_path):
            continue

        rebuild_list = []
        fr = open(minutebar_file_path)
        for line in fr.readlines():
            date_message = line.split(',')[0]
            if date_message[11:13] not in night_filter_list:
                rebuild_list.append(line)

        file_object = open(minutebar_file_path, 'w+')
        file_object.write(''.join(rebuild_list))
        file_object.close()


def holiday_minutebar_rebuild_job():
    for holiday_str in holiday_list:
        __holiday_minutebar_rebuild(holiday_str)


if __name__ == '__main__':
    __holiday_minutebar_rebuild('20161010.csv')