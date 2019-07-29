# -*- coding: utf-8 -*-
import csv
import json
import tarfile
from eod_aps.job.algo_file_build_job import StrategyBasketInfo
from eod_aps.job.pf_position_rebuild_job import pf_position_rebuild_job
from eod_aps.job.update_deposit_server_db_job import update_deposit_server_db_job
from eod_aps.job.upload_docker_models_job import UploadDockerModelFiles
from flask import render_template, request, make_response, jsonify
from flask_login import login_required
from eod_aps.tools.server_manage_tools import *
from eod_aps.tools.ts_manger_tools import ts_update_index
from eod_aps.tools.tradeplat_order_tools import none_order_cancel_tools
from tool_manager import *
from eod_aps.tools.dokcer_manager_tool import DockerManager
from ts_manager_tools import *
from . import tool

server_list = server_constant.get_all_trade_servers()
operation_enums = const.BASKET_FILE_OPERATION_ENUMS
STOCK_SELECTION_FOLDER = const.EOD_CONFIG_DICT['stock_selection_folder']


@tool.route('/eod_tools', methods=['GET', 'POST'])
@login_required
def eod_tools():
    service_list = get_service_list(server_list[0])
    log_show_option = []
    # log_show_option.extend(server_list)
    # log_show_option.extend(os.listdir(datafetcher_messagefile_folder))
    log_show_option.extend(['tool_log', 'eod_log', 'account_log', 'cmd_log'])
    return render_template('eod/eod_tools.html', server_list=server_list, service_list=service_list,
                           log_show_option=log_show_option)


@tool.route('/get_service_status', methods=['GET', 'POST'])
@login_required
def get_service_status():
    config_data = json.loads(request.form.get('config_data'))
    service_list = config_data['service_list']
    status_dict = check_service_status(service_list)
    data = {
        'status_dict': status_dict
    }
    return json.dumps(data)


@tool.route('/server_manager_btn', methods=['GET', 'POST'])
def server_manager_btn():
    params = request.json
    server_name = params.get('server_name')
    manager_option = params.get('option')
    try:
        if manager_option == 'start':
            start_tradeplat(server_name)
        elif manager_option == 'kill':
            stop_tradeplat(server_name)
        elif manager_option == 'quit':
            quit_tradeplat(server_name)
        elif manager_option == 'save pf_position':
            save_pf_position(server_name)
        elif manager_option == 'update pf':
            server_service_rum_cmd(server_name, 'MainFrame', 'update pf')
        elif manager_option == 'update_position':
            update_position_job((server_name,))
        elif manager_option == 'switch_trading_day':
            filter_date_str = params.get('filter_date_str')
            pf_position_rebuild_job((server_name, ), filter_date_str)
        else:
            print 'server_name:%s, option:%s' % (server_name, manager_option)
            return make_response(jsonify(code=100, message=u"参数错误"), 200)
        return_message = 'Server:%s, Option:%s Success!' % (server_name, manager_option)
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, Option:%s Fail!' % (server_name, manager_option)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/service_manager_btn', methods=['GET', 'POST'])
def service_manager_btn():
    params = request.json
    server_name = params.get('server_name')
    service_name = params.get('service_name')
    try:
        manager_option = params.get('option')
        if manager_option == 'start':
            start_server_service(server_name, service_name)
        elif manager_option == 'kill':
            pkill_server_service(server_name, service_name)
        elif manager_option == 'quit':
            quit_server_service(server_name, service_name)
        elif manager_option == 'restart':
            pkill_server_service(server_name, service_name)
            start_server_service(server_name, service_name)
        elif manager_option == 'save_pf_position':
            server_service_rum_cmd(server_name, service_name, 'save pf_position')
        else:
            print 'server_name:%s, option:%s' % (server_name, manager_option)
            return make_response(jsonify(code=100, message=u"参数错误"), 200)
        return_message = 'Server:%s,Service:%s,Option:%s Success!' % (server_name, service_name, manager_option)
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s,Service:%s,Option:%s Fail!' % (server_name, service_name, manager_option)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/deposit_server_manager_btn', methods=['GET', 'POST'])
def deposit_server_manager_btn():
    params = request.json
    server_name = params.get('server_name')
    manager_option = params.get('option')
    try:
        if manager_option == 'update_db_am':
            sql_library_list = ['common', 'portfolio']
            update_deposit_server_db_job((server_name, ), sql_library_list)
        elif manager_option == 'update_db_pm':
            sql_library_list = ['common', 'portfolio', 'om']
            update_deposit_server_db_job((server_name, ), sql_library_list)
        else:
            print 'server_name:%s, option:%s' % (server_name, manager_option)
            return make_response(jsonify(code=100, message=u"参数错误"), 200)
        return_message = 'Server:%s, Option:%s Success!' % (server_name, manager_option)
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, Option:%s Fail!' % (server_name, manager_option)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/docker_manager_btn', methods=['GET', 'POST'])
def docker_manager_btn():
    params = request.json
    server_name = params.get('server_name')
    dockermanager = DockerManager(server_name)
    try:
        manager_option = params.get('option')
        dockermanager.manager_docker(manager_option, 'stkintraday_d1')
        return_message = 'Server:%s,Service:%s,Option:%s Success!' % (server_name, 'docker', manager_option)
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s,Service:%s,Option:%s Fail!' % (server_name, 'docker', manager_option)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/query_log_file', methods=['GET', 'POST'])
def log_manager():
    params = request.json
    server_name = params.get('server_name')
    log_file_name = params.get('log_file_name')
    log_number = params.get('log_number')
    try:
        server_model = server_constant.get_server_model(server_name)
        cmd_list = ['cd %s' % server_model.server_path_dict['tradeplat_log_folder'],
                    'tail -%s %s' % (log_number, log_file_name)
                    ]
        return_message = server_model.run_cmd_str(';'.join(cmd_list))
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, File:%s Query Fail!' % (server_name, log_file_name)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/update_ts_position', methods=['GET', 'POST'])
def update_ts_position():
    params = request.json
    server_name = 'guoxin'
    account_name_list = params.get('account_name_list')
    try:
        for ts_account_name in account_name_list:
            ts_update_index(server_name, ts_account_name)
        return make_response(jsonify(code=200, message=u'更新成功'), 200)
    except Exception:
        print traceback.format_exc()
        return make_response(jsonify(code=100, message=u'更新失败!'), 200)


@tool.route('/reconnent_account', methods=['GET', 'POST'])
def reconnent_account():
    params = request.json
    account_list = params.get('accounts')
    server_name = params.get('server_name')
    try:
        server_model = server_constant.get_server_model(server_name)
        cmd_list = ['cd %s' % server_model.server_path_dict['server_python_folder']]
        for ts_account in account_list:
            cmd = '/home/trader/anaconda2/bin/python screen_tools.py -s OrdGROUP -c "reconnect %s"' % ts_account
            cmd_list.append(cmd)
        server_model.run_cmd_str(';'.join(cmd_list))
        return make_response(jsonify(code=200, message=u'更新成功'), 200)
    except Exception:
        print traceback.format_exc()
        return make_response(jsonify(code=100, message=u'更新失败!'), 200)


@tool.route('/get_account_list', methods=['GET', 'POST'])
def get_account_list():
    params = request.json
    server = params.get('server_name')
    account_name_list = []
    try:
        for (server_name, account_list) in const.EOD_CONFIG_DICT['server_account_dict'].items():
            if server_name != server:
                continue
            else:
                for obj in account_list:
                    account_name = '%s-%s-%s-%s' % (obj.accountname, obj.accounttype, obj.fund_name, obj.accountsuffix)
                    account_name_list.append(account_name)
        return make_response(jsonify(code=200, message=u'', data=account_name_list), 200)
    except Exception:
        print traceback.format_exc()
        return make_response(jsonify(code=100, message=u'', data=account_name_list), 200)


@tool.route('/algo_file_build', methods=['GET', 'POST'])
def algo_file_build():
    params = request.json
    server_name_list = params.get('server_name_list')
    option = params.get('option')

    try:
        for server_name in server_name_list:
            if option == 'change':
                strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Change)
                strategy_basket_info.strategy_basket_file_build()

                error_message_list = strategy_basket_info.check_basket_file()
                if len(error_message_list) > 0:
                    result_message = 'Server:%s,Option:%s Missing Files!' % (','.join(server_name_list), option)
                    return make_response(jsonify(code=100, message=result_message), 200)
            elif option == 'close':
                strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Close)
                strategy_basket_info.strategy_basket_file_build()
            elif option == 'close_bits':
                strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Close_Bits)
                strategy_basket_info.strategy_basket_file_build()
            elif option == 'add':
                add_money = params.get('add_money')
                strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Add)
                if add_money == '' or add_money is None:
                    strategy_basket_info.strategy_basket_file_build()
                else:
                    strategy_basket_info.strategy_basket_file_build(add_money=int(add_money))
            elif option == 'cutdown':
                cut_down_money = params.get('cutdown_money')
                strategy_basket_info = StrategyBasketInfo(server_name, operation_enums.Cutdown)
                if cut_down_money == '' or cut_down_money is None:
                    strategy_basket_info.strategy_basket_file_build()
                else:
                    strategy_basket_info.strategy_basket_file_build(cut_down_money=int(cut_down_money))
            else:
                continue
        result_message = 'Server:%s,Option:%s success!' % (','.join(server_name_list), option)
        return make_response(jsonify(code=200, message=result_message), 200)
    except Exception:
        print traceback.format_exc()
        result_message = 'Server:%s,Option:%s Fail!' % (','.join(server_name_list), option)
        return make_response(jsonify(code=100, message=result_message), 200)


@tool.route('/system_upgrade', methods=['GET', 'POST'])
def system_upgrade():
    params = request.json
    server_name_list = params.get('server_name_list')
    upgrade_file_path = params.get('upgrade_file_path')
    try:
        if not os.path.exists(upgrade_file_path):
            return make_response(jsonify(code=100, message=u"指定升级文件不存在"), 200)

        update_result_list = []
        for change_server_name in server_name_list:
            upgrade_flag = upgrade_server_tradeplat(change_server_name, upgrade_file_path)
            update_result_list.append('Server:%s,Update_Result:%s' % (change_server_name, upgrade_flag))
        return make_response(jsonify(code=200, message='\n'.join(update_result_list)), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, File:%s Upgrade Fail!' % (';'.join(server_name_list), upgrade_file_path)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/cancel_none_order', methods=['GET', 'POST'])
def cancel_none_order():
    params = request.json
    server_name = params.get('server_name')
    limit_time = params.get('limit_time')
    try:
        none_order_cancel_tools(server_name, limit_time)
        result_message = 'Server:%s,Limit_Time:%s None Order Cancel Success!' % (server_name, limit_time)
        return make_response(jsonify(code=200, message=result_message), 200)
    except Exception:
        print traceback.format_exc()
        result_message = 'Server:%s,Limit_Time:%s None Order Cancel Fail!' % (server_name, limit_time)
        return make_response(jsonify(code=100, message=result_message), 200)


@tool.route('/stock_selection_list', methods=['GET', 'POST'])
def stock_selection_list():
    params = request.json
    option = params.get('option')

    table_dict = dict()
    title_list = []
    date_str = date_utils.get_today_str('%Y%m%d')
    stock_servers = server_constant.get_stock_servers()
    for server_name in stock_servers:
        dict_key = '%s|%s' % (option, server_name)
        basket_files_folder = '%s/%s/%s_%s' % (STOCK_SELECTION_FOLDER, server_name, date_str, option)
        sum_info_filename = 'sum_info.csv'
        sum_info_file_path = os.path.join(basket_files_folder, sum_info_filename)
        table_list = []
        if not os.path.exists(sum_info_file_path):
            table_dict[dict_key] = table_list
            continue

        with open(sum_info_file_path) as fr:
            reader = csv.reader(fr)
            for row in reader:
                if reader.line_num == 1:
                    title_list = row
                row_dict = {title_list[i]: val for i, val in enumerate(row)}
                table_list.append(row_dict)
        table_dict[dict_key] = table_list
    result_dict = {'server_list': stock_servers, 'title_list': title_list, 'table_dict': table_dict}
    return make_response(jsonify(code=200, data=result_dict), 200)


@tool.route('/query_trade_log_file', methods=['GET', 'POST'])
def query_trade_log_file():
    params = request.json
    server_name = params.get('server_name')
    path_type = params.get('path_type')

    date_str = date_utils.get_today_str('%Y%m%d')
    time_str = date_utils.get_today_str('%Y%m%d%H%M%S')

    server_model = server_constant.get_server_model(server_name)
    server_folder_path = server_model.server_path_dict['tradeplat_log_folder']
    try:
        cmd_list = ['cd %s' % server_folder_path]
        clear_cmd_list = ['cd %s' % server_folder_path]
        if path_type == u'进程':
            service_name = params.get('service_name')
            tar_file_name = '%s_%s_log.tar.gz' % (service_name, time_str)
            find_key = '%s_%s' % (service_name, date_str)
            cmd_list.append('tar -zcvf %s *%s*.log' % (tar_file_name, find_key))
        elif path_type == u'关键字':
            find_key = params.get('find_key')
            temp_file_name = '%s_%s.log' % (find_key, date_str)
            cmd_list.append('grep %s *%s*.log > %s' % (find_key, date_str, temp_file_name))
            clear_cmd_list.append('rm %s' % temp_file_name)

            tar_file_name = '%s_%s_log.tar.gz' % (find_key, time_str)
            cmd_list.append('tar -zcvf %s %s' % (tar_file_name, temp_file_name))
        else:
            return make_response(jsonify(code=100, message='Error path_type'), 200)

        clear_cmd_list.append('rm %s' % tar_file_name)
        server_model.run_cmd_str(';'.join(cmd_list))

        local_save_path = const.EOD_CONFIG_DICT['log_backup_folder_template'] % server_name
        if not os.path.exists(local_save_path):
            os.makedirs(local_save_path)

        local_file_path = '%s/%s' % (local_save_path, tar_file_name)
        server_model.download_file('%s/%s' % (server_folder_path, tar_file_name),
                                   '%s' % local_file_path)
        return_message = u'Save Path:%s' % local_file_path
        # 解压
        t = tarfile.open(local_file_path)
        t.extractall(local_save_path)
        t.close()
        os.remove(local_file_path)

        server_model.run_cmd_str(';'.join(clear_cmd_list))
        return make_response(jsonify(code=200, message=return_message), 200)
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, File:%s Query Fail!' % (server_name, server_folder_path)
        return make_response(jsonify(code=100, message=return_message), 200)


@tool.route('/return_file_data', methods=['GET', 'POST'])
def return_file_data():
    params = request.json
    server_name = params.get('server_name')
    service_name = params.get('service_name')
    path_type = params.get('path_type')

    date_str = date_utils.get_today_str('%Y%m%d')
    time_str = date_utils.get_today_str('%Y%m%d%H%M%S')

    server_model = server_constant.get_server_model(server_name)
    server_folder_path = server_model.server_path_dict['tradeplat_log_folder']
    try:
        cmd_list = ['cd %s' % server_folder_path]
        clear_cmd_list = ['cd %s' % server_folder_path]
        if path_type == u'进程':
            tar_file_name = '%s_%s_log.tar.gz' % (service_name, time_str)
            find_key = '%s_%s' % (service_name, date_str)
            cmd_list.append('tar -zcvf %s *%s*.log' % (tar_file_name, find_key))
        elif path_type == u'关键字':
            find_key = params.get('find_key')
            temp_file_name = '%s_%s.log' % (find_key, date_str)
            cmd_list.append('grep %s *%s*.log > %s' % (find_key, date_str, temp_file_name))
            clear_cmd_list.append('rm %s' % temp_file_name)

            tar_file_name = '%s_%s_log.tar.gz' % (find_key, time_str)
            cmd_list.append('tar -zcvf %s %s' % (tar_file_name, temp_file_name))
        else:
            response = make_response(jsonify(code=100, message='Error path_type'), 200)
            response.headers['file_name'] = "%s_%s_error.log" % (server_name, service_name)
            return response

        clear_cmd_list.append('rm %s' % tar_file_name)
        server_model.run_cmd_str(';'.join(cmd_list))

        result = server_model.read_file('%s/%s' % (server_folder_path, tar_file_name))

        server_model.run_cmd_str(';'.join(clear_cmd_list))
        response = make_response(result)
        response.headers['file_name'] = "%s_%s" % (server_name, tar_file_name)
        return response
    except Exception:
        print traceback.format_exc()
        return_message = 'Server:%s, File:%s Query Fail!' % (server_name, server_folder_path)
        response = make_response(jsonify(code=100, message=return_message, ), 200)
        response.headers['file_name'] = "%s_%s_error.log" % (server_name, service_name)
        return response


@tool.route('/encrypt_decrypt_str', methods=['GET', 'POST'])
def encrypt_decrypt_str():
    aggregator_server_name = 'aggregator'
    params = request.json
    encrypt_str = params.get('encrypt_str')
    decrypt_str = params.get('decrypt_str')
    handle_type = params.get('handle_type')
    server_model = server_constant.get_server_model(aggregator_server_name)
    try:
        if handle_type == u'加密':
            cmd_list = ['cd /home/yansheng/apps/Aggregator/encrypt_decrypt',
                        './Encrypt %s' % encrypt_str,
                        ]
        elif handle_type == u'解密':
            cmd_list = ['cd /home/yansheng/apps/Aggregator/encrypt_decrypt',
                        './Decrypt %s' % decrypt_str,
                        ]
        else:
            return make_response(jsonify(code=100, message='处理失败', data=''), 200)
        return_data = server_model.run_cmd_str(';'.join(cmd_list))
        return_message = '处理成功' if return_data != '' else '无处理结果，请重新输入...'
        return make_response(jsonify(code=200, message=return_message, data=return_data), 200)
    except Exception:
        return make_response(jsonify(code=100, message='处理失败', data=return_data), 200)


@tool.route('/upload_models_files', methods=['GET', 'POST'])
def upload_models_files():
    params = request.json
    server_name_list = params.get('server_name_list')
    include_list = params.get('include_list')
    model_flag = bool(params.get('model_flag'))
    tar_flag = bool(params.get('tar_flag'))

    index_num = 1
    upload_docker_model_files = UploadDockerModelFiles(server_name_list, index_num)
    stock_include_flag = True if 'Stock' in include_list else False
    index_include_flag = True if 'Index' in include_list else False

    upload_docker_model_files.upload_models_files(stock_include_flag, index_include_flag, model_flag, tar_flag)
    return make_response(jsonify(code=200, message='models文件上传完毕，上传结果参见邮件.'), 200)
