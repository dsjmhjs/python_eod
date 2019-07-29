# -*- coding: utf-8 -*-
import os


base_file_path = 'H:/data_history/VEX/quotes_base'
save_file_path = 'E:/market_file/quotes_base'


def  file_filter_tools():
    for ticker_folder_name in os.listdir(base_file_path):
        for date_file_name in os.listdir(base_file_path + '/' + ticker_folder_name):
            if date_file_name != '20160824.csv':
                continue
            f = open('%s/%s/%s' % (base_file_path, ticker_folder_name, date_file_name), 'r')
            save_quote_list = []
            try:
                for line in f.xreadlines():
                    line_item = line.split(',')
                    if line_item[0] > '2016-08-24 08:59:00.0000000':
                        save_quote_list.append(line)
            finally:
                f.close()
            file_path = '%s/%s' % (save_file_path, ticker_folder_name)
            if not os.path.exists(file_path):
                os.mkdir(file_path)
            file_object = open(file_path + '/' + '2016-08-24_1.csv', 'w')
            file_object.write(''.join(save_quote_list))
            file_object.close()



if __name__ == '__main__':
    file_filter_tools()
