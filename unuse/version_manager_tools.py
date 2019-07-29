# -*- coding: utf-8 -*-
import os
from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser()

    parser.add_argument(
        "-b",
        "--base_path",
        dest="base_path",
        help='project base path',
        default='/home/yangzhoujie/code'
    )

    parser.add_argument(
        "-p",
        "--project",
        dest="project",
        help='input project name',
        default='libatp'
    )

    #version/quote
    parser.add_argument(
        "-t",
        "--type",
        dest="type",
        help='input result type',
        default='version'
    )

    options = parser.parse_args()
    return options


def __build_depend(project_name, node_name_list):
    project_so_dict = dict()

    project_build_path = '%s/%s/%s' % (BASE_CODE_PATH, 'build64_release', project_name)
    find_so_cmd = os.popen('cd %s;find -name "*.so"' % project_build_path)
    cmd_message = find_so_cmd.read()

    so_file_list = cmd_message.split('\n')
    so_file_list.extend(node_name_list)
    for so_file_str in so_file_list:
        if so_file_str is None or so_file_str == '':
            continue

        so_file_name = os.path.basename(so_file_str)
        so_file_path = '%s/%s' % (project_build_path, so_file_str)

        if not os.path.exists(so_file_path):
            continue

        find_depend_cmd = os.popen('cd %s;ldd %s' % (BASE_CODE_PATH, so_file_path))
        depend_so_list = find_depend_cmd.read()

        depend_so_message = []
        for depend_so_str in depend_so_list.split('\n'):
            if depend_so_str is None or depend_so_str == '':
                continue
            if '=>' in depend_so_str:
                continue
            if '/lib64/ld-linux-x86-64.so.2' in depend_so_str:
                continue

            depend_so_path = depend_so_str.split(' ')[0].strip()
            depend_so_name = os.path.basename(depend_so_path)
            if 'build64_release' in depend_so_str:
                depend_so_path = '%s/%s' % (BASE_CODE_PATH, depend_so_path)

            md5_cmd = os.popen('md5sum %s' % depend_so_path)
            md5_value = md5_cmd.read().split(' ')[0]
            depend_so_message.append('%s(%s)' % (depend_so_name, md5_value))

        so_md5_cmd = os.popen('md5sum %s' % so_file_path)
        so_md5_value = so_md5_cmd.read().split(' ')[0]
        dict_key = '%s(%s)' % (so_file_name, so_md5_value)
        project_so_dict[dict_key] = depend_so_message
    return project_so_dict


def __build_version(project_name):
    project_path = '%s/%s' % (BASE_CODE_PATH, project_name)
    project_git_head_path = '%s/.git/HEAD' % project_path

    cat_head_cmd = os.popen('cat %s' % project_git_head_path)
    git_head_value = cat_head_cmd.read()

    head_name = git_head_value.split('/')[-1].strip()

    project_git_version_path = '%s/.git/refs/heads/%s' % (project_path, head_name)
    cat_version_cmd = os.popen('cat %s' % project_git_version_path)
    git_version_value = cat_version_cmd.read().strip()
    return head_name, git_version_value


def __get_version_def(project_name):
    project_path = '%s/%s' % (BASE_CODE_PATH, project_name)

    version_def_str = 'None'
    version_def_file_path = '%s/VERSION.def' % project_path
    if not os.path.exists(version_def_file_path):
        print 'Miss file:', version_def_file_path
    else:
        with open(version_def_file_path) as fr:
            version_def_str = fr.read().replace('\n', '')

    environment_str = 'None'
    environment_file_path = '%s/ENVIRONMENT' % project_path
    if not os.path.exists(environment_file_path):
        print 'Miss file:', environment_file_path
    else:
        with open(environment_file_path) as fr:
            environment_str = fr.read().replace('\n', '')
    return '%s_%s' % (version_def_str, environment_str)


def __get_node_name_list(project_name):
    build_file_path = '%s/%s/BUILD' % (BASE_CODE_PATH, project_name)
    cat_cmd = os.popen('cat %s' % build_file_path)
    build_file_message = cat_cmd.read()

    node_name_list = []
    for file_item in build_file_message.split('\n'):
        if 'name =' not in file_item:
            continue
        node_name = file_item.split("'")[1]
        node_name_list.append(node_name)
    return node_name_list


def version_manager(base_code_path, project_name):
    global BASE_CODE_PATH
    BASE_CODE_PATH = base_code_path
    head_name, git_version_value = __build_version(project_name)
    version_def_str = __get_version_def(project_name)
    version_message = '%s;%s;%s(%s)' % (project_name, head_name, version_def_str, git_version_value)

    node_name_list = __get_node_name_list(project_name)
    project_so_dict = __build_depend(project_name, node_name_list)

    output_message_list = ['[version]', '%s=%s' % (project_name, version_message), '', '', '[dependency]']
    for (so_file_name, depend_so_list) in project_so_dict.items():
        output_message_list.append('%s=%s' % (so_file_name, ';'.join(depend_so_list)))
        output_message_list.append('')
    print '\n'.join(output_message_list)


def __build_quote(project_name, node_name_list):
    project_quote_set = set()

    project_build_path = '%s/%s/%s' % (BASE_CODE_PATH, 'build64_release', project_name)
    find_so_cmd = os.popen('cd %s;find -name "*.so"' % project_build_path)
    cmd_message = find_so_cmd.read()

    so_file_list = cmd_message.split('\n')
    so_file_list.extend(node_name_list)
    for so_file_str in so_file_list:
        if so_file_str is None or so_file_str == '':
            continue

        so_file_name = os.path.basename(so_file_str)
        so_file_path = '%s/%s' % (project_build_path, so_file_str)

        if not os.path.exists(so_file_path):
            continue

        find_depend_cmd = os.popen('cd %s;ldd %s' % (BASE_CODE_PATH, so_file_path))
        depend_so_list = find_depend_cmd.read()

        for depend_so_str in depend_so_list.split('\n'):
            if depend_so_str is None or depend_so_str == '':
                continue
            if '=>' not in depend_so_str:
                continue

            quote_file_path = depend_so_str.split('=>')[1].split('(')[0].strip()
            project_quote_set.add(quote_file_path)
    return project_quote_set


def __get_project_node_dict():
    project_node_dict = dict()

    find_build_cmd = os.popen("cd %s;find %s -name 'BUILD'" % (BASE_CODE_PATH, BASE_CODE_PATH))
    build_list = find_build_cmd.read()
    for build_str in build_list.split('\n'):
        if build_str is None or build_str == '':
            continue
        if 'BUILD' not in  build_str:
            continue

        cat_cmd = os.popen('cd %s;cat %s' % (BASE_CODE_PATH, build_str))
        build_file_message = cat_cmd.read()

        node_name_list = []
        for file_item in build_file_message.split('\n'):
            if 'name =' not in file_item:
                continue
            node_name = file_item.split("'")[1]
            node_name_list.append(node_name)
        project_name = build_str.split('/')[-2]
        project_node_dict[project_name] = node_name_list
    return project_node_dict


def quote_manager(base_code_path):
    global BASE_CODE_PATH
    BASE_CODE_PATH = base_code_path

    project_node_dict = __get_project_node_dict()
    out_put_set = set()
    for (project_name, node_name_list) in project_node_dict.items():
        project_quote_set = __build_quote(project_name, node_name_list)
        out_put_set.update(project_quote_set)

    output_message_list = ['\n'.join(out_put_set), '']
    print '\n'.join(output_message_list)


if __name__ == '__main__':
    options = parse_arguments()
    base_code_path = options.base_path
    project_name = options.project
    input_type = options.type
    if input_type == 'version':
        version_manager(base_code_path, project_name)
    elif input_type == 'quote':
        quote_manager(base_code_path)

