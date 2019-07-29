# -*- coding: utf-8 -*-
import os
from datetime import datetime

config_file_folder_sh = 'Z:/temp/longling/IntradayStrategy/config/FCT_SH'
ticker_str_sh = '600051,600112,600237,600708,600765,600816,601388,600238,600291,600359,600691,601009,600064,600077,\
600090,600665,600681,600683,600791,601668,600026,600048,600052,600060,600325,600503,600658,600751,600986,601166,600287,\
600295,600335,600757,600835,600970,603001'

config_file_folder_sz = 'Z:/temp/longling/IntradayStrategy/config/FCT_SZ'
ticker_str_sz = '000586,000597,000666,000789,000835,002045,002090,002566,002667,300106,300167,300313,300401,000159,\
000701,000900,002133,002321,002452,002519,300029,300341,000036,000066,000587,002016,002567,000002,000059,000521,000631,\
000863,002048,002157,002208,002454,300063,300240,000501,000667,000892,000897,002394,002401,002420,300282'

save_file_folder = 'Z:/temp/longling/IntradayStrategy/config/cmd_file'

cmd_template = 'BackTest --StratsName=StkIntraDay --WatchList=%s --ParaList=%s --Parameter=[Account]1:0:0;\
[%s_%s_weight60s]0.002:0.01:0.0005; --StartTime=%s --EndTime=%s --AssemblyName=libstk_intra_day_strategy --Parallel=1 '


def start_build():
    for (file_folder, ticker_str) in [(config_file_folder_sh, ticker_str_sh), (config_file_folder_sz, ticker_str_sz)]:
        __build_config_file(file_folder, ticker_str)


def __build_config_file(file_folder, ticker_str):
    now_date_str = datetime.now().strftime('%Y-%m-%d')

    para_list = []
    for file_name in os.listdir(file_folder):
        if '.csv' not in file_name:
            continue

        # print 'file_name:', file_name
        fr = open('%s/%s' % (file_folder, file_name))
        i = 0
        for line in fr.readlines():
            if i == 0:
                title_items = line.replace('\n', '').split(',')
                i += 1
                continue
            value_items = line.replace('\n', '').split(',')

            para_str = file_name.split('.')[0]
            for a in range(0, len(title_items)):
                para_str += '_%s%s' % (title_items[a].replace('"', ''), value_items[a])
            para_list.append(para_str)

    for ticker in ticker_str.split(','):
        cmd_str_list = []
        for para in para_list:
            cmd_str = cmd_template % (ticker, para, ticker, para, now_date_str, now_date_str)
            cmd_str_list.append(cmd_str)
        save_cmd_file(ticker, now_date_str, cmd_str_list)


def save_cmd_file(ticker, date_str, content):
    file_path = '%s/command_file_%s_%s.txt' % (save_file_folder, ticker, date_str)
    file_object = open(file_path, 'w+')
    file_object.write('\n'.join(content))
    file_object.close()


if __name__ == '__main__':
    start_build()
