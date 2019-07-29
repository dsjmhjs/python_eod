# -*- coding: utf-8 -*-
import os
import zipfile
import shutil
import tarfile


class FileUtils(object):
    """
        文件处理工具类
    """
    base_file_path = None

    def __init__(self, base_file_path):
        self.base_file_path = base_file_path

    def filter_file(self, *filter_key_items):
        filter_price_files = []
        for rt, dirs, files in os.walk(self.base_file_path):
            for search_file in files:
                find_flag = True
                for filter_key in filter_key_items:
                    if filter_key not in search_file:
                        find_flag = False

                if find_flag:
                    filter_price_files.append(search_file)
        filter_price_files.sort()
        return filter_price_files

    @staticmethod
    def zip_file(file_list, filename_zip):
        zf = zipfile.ZipFile(filename_zip, "w", zipfile.zlib.DEFLATED)
        for file_path_full in file_list:
            (file_path, file_name) = os.path.split(file_path_full)
            zf.write(file_path, file_name)
        zf.close()

    @staticmethod
    def unzip_file(filename_zip, unzip_dir):
        if not os.path.exists(unzip_dir):
            os.mkdir(unzip_dir, 0777)
        zfobj = zipfile.ZipFile(filename_zip)
        for name in zfobj.namelist():
            name = name.replace('\\', '/')
            if name.endswith('/'):
                os.mkdir(os.path.join(unzip_dir, name))
            else:
                ext_filename = os.path.join(unzip_dir, name)
                ext_dir = os.path.dirname(ext_filename)
                if not os.path.exists(ext_dir):
                    os.mkdir(ext_dir, 0777)

                with open(ext_filename, 'wb') as fr:
                    fr.write(zfobj.read(name))

    @staticmethod
    def make_targz(output_filename, source_dir):
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    def clear_folder(self):
        if os.path.exists(self.base_file_path):
            shutil.rmtree(self.base_file_path)
        os.mkdir(self.base_file_path)


if __name__ == '__main__':
    FileUtils.make_targz('D:\work\daily\models_example.tar.gz', 'D:\work\daily')
