# -*- coding: utf-8 -*-
import re
import ConfigParser
from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser()

    parser.add_argument(
        "-b",
        "--build_file",
        dest="build_file",
        help='build_file path',
        default=''
    )

    parser.add_argument(
        "-v",
        "--version file",
        dest="version_file",
        help='version file path',
        default=''
    )

    parser.add_argument(
        "-v2",
        "--version file2",
        dest="version_file2",
        help='version file2 path',
        default=''
    )

    options = parser.parse_args()
    return options


# 'Z:/yansheng/release/JenkinsBuild/uat/production/binary/version_20180316_101301.txt'
def __read_version_files(version_file_path):
    version_dict = dict()
    cp = ConfigParser.SafeConfigParser()
    cp.read(version_file_path)

    for version_key in cp.options('version'):
        version_value_base = cp.get('version', version_key)
        try:
            m = re.match(".*\((.*)\).*", version_value_base)
            version_value = m.group(1)
        except AttributeError:
            version_value = ''
        version_dict[version_key] = version_value
    return version_dict


def __compare_version_dict1(version_dict1, version_dict2):
    print 'Dict1:'
    for k, v in version_dict1.items():
        print '%s=%s' % (k, v)

    print 'Dict2:'
    for k, v in version_dict2.items():
        print '%s=%s' % (k, v)

    print 'Diff:'
    for k, v in version_dict1.items():
        if k in version_dict2:
            if v != version_dict2[k]:
                print '%s=%s,%s' % (k, v, version_dict2[k])
        else:
            print '%s=%s,%s' % (k, v, '')


def __compare_version_dict2(version_dict1, version_dict2):
    print 'Dict1:'
    for k, v in version_dict1.items():
        print '%s=%s' % (k, v)

    print 'Dict2:'
    for k, v in version_dict2.items():
        print '%s=%s' % (k, v)

    print 'Diff:'
    version_key_set = set()
    version_key_set.update(version_dict1.keys())
    version_key_set.update(version_dict2.keys())
    for version_key in list(version_key_set):
        if version_key in version_dict1:
            version_value1 = version_dict1[version_key]
        else:
            version_value1 = ''

        if version_key in version_dict2:
            version_value2 = version_dict2[version_key]
        else:
            version_value2 = ''

        if version_value1 != version_value2:
            print '%s=%s,%s' % (version_key, version_value1, version_value2)


def compare_version1(build_file, version_file):
    version_dict1 = __read_version_files(build_file)
    version_dict2 = __read_version_files(version_file)
    __compare_version_dict1(version_dict1, version_dict2)


def compare_version2(version_file, version_file2):
    version_dict1 = __read_version_files(version_file)
    version_dict2 = __read_version_files(version_file2)
    __compare_version_dict2(version_dict1, version_dict2)


if __name__ == '__main__':
    options = parse_arguments()
    input_build_file = options.build_file
    input_version_file = options.version_file
    input_version_file2 = options.version_file2

    if input_build_file and input_version_file:
        print input_build_file, input_version_file
        compare_version1(input_build_file, input_version_file)

    if input_version_file and input_version_file2:
        compare_version2(input_version_file, input_version_file2)
