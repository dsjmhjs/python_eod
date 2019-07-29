# -*- coding: utf-8 -*-
import os
import shutil


# def __read_pcf_file_csv(csv_file_path):
#     # index_SHSZ300List = []
#     # index_SSE50List = []
#     # index_SH000905List = []
#     #
#     #
#     # print 'read file:%s' % csv_file_path
#     # csv_file = file(csv_file_path, 'rb')
#     # reader = csv.reader(csv_file)
#     # for line in reader:
#     #     if ('000300' == line[0]) and (u'次日权重' == line[5].decode("gbk")):
#     #         ticker = '%06s' % line[2]
#     #         weight = line[14]
#     #         index_SHSZ300List.append([ticker, weight])
#     #
#     #     elif (line[0] in '000016|000905') and (u'次日权重' == line[4].decode("gbk")):
#     #         ticker = '%06s' % line[2]
#     #         weight = line[6]
#     #         if '000016' == line[0]:
#     #             index_SSE50List.append([ticker, weight])
#     #         elif '000905' == line[0]:
#     #             index_SH000905List.append([ticker, weight])
#
#     base_folder = 'Z:/temp/data_share/index_weight'
#     for date_name in os.listdir(base_folder):
#         file_name = u'上证50权重信息-%s.csv' % date_name
#         rename = 'ShanghaiStockExchange50IndexWeightInfo-%s.csv' % date_name
#
#         file_path = '%s/%s/%s' % (base_folder, date_name, file_name)
#         rename_file_path = '%s/%s/%s' % (base_folder, date_name, rename)
#         if os.path.exists(file_path):
#             shutil.copy(file_path, rename_file_path)
#             # print file_path
#
#
if __name__ == '__main__':
    # csv_file_path = u'Z:/temp/data_share/index_weight/20150625/上证50权重信息-20150625.csv'
    # __read_pcf_file_csv(csv_file_path)
    base_source_path = 'Z:/temp/data_share/index_weight'
    base_target_path = 'E:/index_weight'

    for date_folder_str in os.listdir(base_source_path):
        try:
            if int(date_folder_str) < 20181006:
                continue
        except Exception:
            continue
        source_path = os.path.join(base_source_path, date_folder_str)
        target_path = os.path.join(base_target_path, date_folder_str)
        if not os.path.exists(target_path):
            os.mkdir(target_path)

        for file_name in os.listdir(source_path):
            if 'ShanghaiShenzhen300IndexWeightInfo' not in file_name:
                continue
            shutil.copy(os.path.join(source_path, file_name),
                        os.path.join(target_path, file_name))


