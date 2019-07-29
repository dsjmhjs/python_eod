# -*- coding: utf-8 -*-
import random
from flask import render_template, request, jsonify, make_response
from flask import Flask
from flask_apscheduler import APScheduler
from flask_login import login_required
from flask_cors import CORS
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES
from eod_aps.task.maintain_server_task import *
from eod_aps.task.server_manage_task import *
from eod_aps.task.ysquant_manage_task import *
from eod_aps.task.eod_check_task import *
from cfg import *
from collections import OrderedDict
from copy_file import *


class Config(object):
    JOBS = [

        # -------------------------------------早盘前任务----------------------------------
        {'id': 'copy_volume_profile', 'func': copy_volume_profile, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "20", 'name': u'copy volume_profile 文件'},

        {'id': 'copy_conf_job', 'func': copy_conf_job, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '08', 'minute': "50", 'name': u'copy conf 文件'},

        {'id': 'db_pre_update_am', 'func': db_pre_update_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "10", 'name': u'数据库预更新'},

        {'id': 'reload_pickle_data', 'func': reload_pickle_data, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "28", 'name': u'缓存数据重新加载'},

        {'id': 'build_calendarma_transfer_parameter', 'func': build_calendarma_transfer_parameter,
         'trigger': 'cron', 'day_of_week': '0-4', 'hour': '09', 'minute': "33", 'name': u'更新换月参数'},

        {'id': 'order_check', 'func': order_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "34", 'name': u'隔夜单检查'},

        {'id': 'start_update_position', 'func': start_update_position, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "35", 'name': u'持仓更新'},

        {'id': 'start_update_future_price', 'func': start_update_future_price, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "39", 'name': u'期货行情更新'},

        {'id': 'start_update_stock_price', 'func': start_update_stock_price, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "41", 'name': u'股票行情更新'},

        {'id': 'db_check_am', 'func': db_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "42", 'name': u'早盘任务检查'},

        {'id': 'oma_quota_build', 'func': oma_quota_build, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "44", 'name': u'oma_quota数据生成'},

        {'id': 'start_server_am', 'func': start_server_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "45", 'name': u'交易系统启动'},

        {'id': 'start_aggregator_am', 'func': start_aggregator_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "46", 'name': u'Aggregator启动'},

        {'id': 'start_server_strategy_am', 'func': start_server_strategy_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "48", 'name': u'策略启动'},

        {'id': 'strategy_deeplearning_init', 'func': strategy_deeplearning_init, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "52", 'name': u'日内策略初始化'},

        {'id': 'after_start_check_am', 'func': after_start_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "56", 'name': u'系统启动后检查'},

        {'id': 'strategy_multifactor_init', 'func': strategy_multifactor_init, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "57", 'name': u'多因子策略初始化'},

        {'id': 'tradeplat_init_index', 'func': tradeplat_init_index, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '09', 'minute': "00", 'name': u'Tradeplat配置初始化'},

        {'id': 'special_ticker_report', 'func': special_ticker_report, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "03", 'name': u'今日需关注股票报告'},

        {'id': 'pf_real_position_check_am', 'func': pf_real_position_check_am, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "05", 'name': u'真实仓位和策略仓位比对'},

        {'id': 'save_aggregator_message', 'func': save_aggregator_message, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "10", 'name': u'缓存aggregator数据'},

        {'id': 'restart_mktdtcenter_service', 'func': restart_mktdtcenter_service, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "13", 'name': u'行情中心服务重启'},

        # {'id': 'mkt_center_log_check', 'func': mkt_center_log_check, 'trigger': 'cron',
        #  'day_of_week': '0-4', 'hour': '09', 'minute': "15", 'name': u'国信日志校验'},

        {'id': 'server_status_check', 'func': server_status_check, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "20", 'name': u'服务器状态检查'},

        {'id': 'risk_calculation', 'func': risk_calculation, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "20", 'name': u'计算风控'},

        {'id': 'alpha_calculation', 'func': alpha_calculation, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '10', 'minute': "25", 'name': u'计算绩效'},

        {'id': 'stop_service_pm', 'func': stop_service_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "30", 'name': u'交易系统关闭[PM]'},

        {'id': 'kill_aggregator_pm', 'func': kill_aggregator_pm, 'trigger': 'cron',
         'day_of_week': '0-4', 'hour': '17', 'minute': "32", 'name': u'关闭Aggregator[PM]'},
    ]

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': True,  # 积攒的任务只跑一次
        'max_instances': 1000,  # 支持1000个实例并发
        'misfire_grace_time': 600  # 600秒的任务超时容错
    }
    SCHEDULER_API_ENABLED = True


def err_listener(ev):
    try:
        if ev.code == EVENT_JOB_MAX_INSTANCES:
            error_msg = 'Job_Id:%s\n Max Instances Error!' % str(ev.job_id)
        elif ev.exception:
            error_msg = 'Scheduled_Run_Time:%s\nRetval:%s\nException:%s\nTraceback:%s Error.' \
                        % (str(ev.scheduled_run_time), str(ev.retval), str(ev.exception), str(ev.traceback))
        else:
            error_msg = 'Job_Id:%s\nScheduled_Run_Time:%s Unknown Error!' % (str(ev.job_id), str(ev.scheduled_run_time))
    except AttributeError:
        error_msg = traceback.format_exc()
    custom_log.log_error_task(error_msg)
    email_utils2.send_email_group_all('[ERROR]Apscheduler Job!', error_msg)


app = Flask(__name__)
CORS(app, supports_credentials=True)
app.secret_key = 'FYlKCBmQWwPzfDI4'

from templates.account import account as account_blueprint

app.register_blueprint(account_blueprint, url_prefix='/account')

from templates.eod import eod as eod_blueprint

app.register_blueprint(eod_blueprint, url_prefix='/eod')

from templates.critical_job import critical_job as critical_job_blueprint

app.register_blueprint(critical_job_blueprint, url_prefix='/critical_job')

from templates.report import report as report_blueprint

app.register_blueprint(report_blueprint, url_prefix='/report')

from templates.tool_list import tool as tool_blueprint

app.register_blueprint(tool_blueprint, url_prefix='/tool_list')

from templates.summary import summary as summary_blueprint

app.register_blueprint(summary_blueprint, url_prefix='/summary')

from templates.cta import cta as cta_blueprint

app.register_blueprint(cta_blueprint, url_prefix='/cta')

from templates.fund import fund as fund_blueprint

app.register_blueprint(fund_blueprint, url_prefix='/fund')

from templates.system import system as system_blueprint

app.register_blueprint(system_blueprint, url_prefix='/system')

from templates.display_module import display_module as display_module_blueprint

app.register_blueprint(display_module_blueprint, url_prefix='/display_module')

from templates.statistic_module import statistic_module as statistic_module

app.register_blueprint(statistic_module, url_prefix='/statistic_module')

app.config.from_object(Config())
app.config['SECRET_KEY'] = 'MY_KEY'

scheduler = APScheduler()
scheduler.init_app(app)
scheduler._logger = custom_log.get_logger('root')
scheduler.add_listener(err_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)


# @app.route('/job', methods=['GET', 'POST'])
# @login_required
# def apscheduler_jobs():
#     jobs_array = scheduler.get_jobs()
#     jobs_lists = []
#
#     for obj in jobs_array:
#         dic = {'id': obj.id, 'name': obj.name}
#         func_name = obj.id
#         if func_name in const.JOB_START_TIME_DICT:
#             dic['start_time'] = const.JOB_START_TIME_DICT[func_name]
#
#         if func_name in const.JOB_END_TIME_DICT:
#             dic['end_time'] = const.JOB_END_TIME_DICT[func_name]
#
#         if hasattr(obj, 'next_run_time') and obj.next_run_time is not None:
#             dic['next_run_time'] = obj.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
#         else:
#             dic['next_run_time'] = '0000-00-00 00:00:00'
#         jobs_lists.append(dic)
#     jobs_lists.sort(key=lambda item: item['next_run_time'][11:])
#     return render_template('eod/jobs.html',
#                            jobs=jobs_lists,
#                            now_date_str=date_utils.get_today_str('%Y-%m-%d %H:%M:%S'),
#                            next_date_str=date_utils.get_next_trading_day('%Y-%m-%d'))


# ========================= manual task ==============================
@app.route('/task_manager', methods=['GET', 'POST'])
def task_manager():
    params = request.json
    task_id = params.get('task_id')
    option = params.get('option')

    custom_log.log_info_task('Job[%s]Manual Run.============================' % task_id)
    try:
        if option == 'pause':
            # scheduler.shutdown(False)
            # scheduler.start()
            scheduler.pause_job(task_id)
        elif option == 'restart':
            scheduler.run_job(task_id)
        elif option == 'resume':
            scheduler.resume_job(task_id)
        complete_msg = 'Task:%s,Option:%s Success!' % (task_id, option)
    except Exception:
        custom_log.log_info_task(traceback.format_exc())
        complete_msg = 'Task:%s,Option:%s Fail!' % (task_id, option)
        return make_response(jsonify(code=100, message=complete_msg), 200)
    return make_response(jsonify(code=200, message=complete_msg), 200)


@app.route('/query_run_log')
@login_required
def query_run_log():
    query_log_path = 'log/eod_task.log'
    with open(query_log_path, 'rb') as fr:
        run_long_list = [x for x in fr.readlines()]
    return jsonify(run_long_list=run_long_list[-20:])


@app.route('/query_apscheduler_jobs', methods=['GET', 'POST'])
def query_apscheduler_jobs():
    params = request.json
    search_id = params.get('search_id')

    jobs_array = scheduler.get_jobs()
    apscheduler_job_lists = []

    for obj in jobs_array:
        if search_id and search_id not in obj.id:
            continue
        dic = {'id': obj.id, 'name': obj.name}
        func_name = obj.id
        if func_name in const.JOB_START_TIME_DICT:
            dic['start_time'] = const.JOB_START_TIME_DICT[func_name]

        if func_name in const.JOB_END_TIME_DICT:
            dic['end_time'] = const.JOB_END_TIME_DICT[func_name]

        if hasattr(obj, 'next_run_time') and obj.next_run_time is not None:
            dic['next_run_time'] = obj.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            dic['next_run_time'] = '0000-00-00 00:00:00'
        apscheduler_job_lists.append(dic)
    apscheduler_job_lists.sort(key=lambda item: item['next_run_time'][11:])
    query_result = {'data': apscheduler_job_lists}
    return make_response(jsonify(code=200, data=query_result), 200)


my_cookie = dict()


@app.route('/login', methods=['GET', 'POST'])
def login():
    info = request.json
    if info is None:
        return make_response(jsonify(code=405, message="非法登陆!"))
    else:
        user_id = info.get('name')
        password = info.get('password')
        server_model = server_constant.get_server_model('host')
        session_job = server_model.get_db_session('jobs')
        query_sql = "select password, role_id from `jobs`.`user_list` where user_id='%s'" % user_id
        user_info_item = session_job.execute(query_sql).first()
        if not user_info_item or user_info_item[0] != password:
            return make_response(jsonify(code=404, message=u"用户名或密码错误，登陆失败!"))
        else:
            query_sql = "select menu_id_list from `jobs`.`role_list` where id='%s'" % user_info_item[1]
            menu_ids_str = session_job.execute(query_sql).first()[0]
            menu_id_list = menu_ids_str.split(';')

            menu_dict = OrderedDict()
            url_list = []
            query_sql = "select `subject_name`,`name`,`url` from jobs.menu_list where id in (%s) order by weight" % \
                        ','.join(menu_id_list)
            for list_item in session_job.execute(query_sql):
                url_list.append(list_item[2])
                menu_dict.setdefault(list_item[0], []).append((list_item[1], list_item[2]))
            subject_list = []
            for (subject_name, sub_list) in menu_dict.items():
                subject_list.append((subject_name, sub_list))

            custom_key = hex(random.randint(268435456, 4294967295))
            key = custom_key[2:len(custom_key) - 1]
            key = '%s|%s' % (user_id, key)
            token = [key, subject_list]
            my_cookie[key] = url_list
            rst = {'token': token, 'role_id': user_info_item[1]}
            return make_response(jsonify(code=200, message=u"恭喜你，登陆成功!", data=rst))


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    info = request.json
    if info is None:
        return make_response(jsonify(code=200, message=u"您已成功注销!"))
    else:
        login_key = info.get('key')

        if login_key in my_cookie:
            my_cookie.pop(login_key, None)
            return make_response(jsonify(code=200, message=u"您已成功注销!"))
        else:
            return make_response(jsonify(code=200, message=u"您已成功注销!"))


@app.route('/authentication', methods=['GET', 'POST'])
def authentication():
    info = request.json
    if info is None:
        return make_response(jsonify(code=401, message=u"对不起，您未登录!"))
    else:
        input_key = info.get('key')
        input_to = info.get('to')
        if input_key in my_cookie:
            routes = my_cookie[input_key]
            if input_to in routes:
                return make_response(jsonify(code=200, message=u"恭喜你，访问成功!"))
            else:
                return make_response(jsonify(code=404, message=u"对不起，访问权限不足!"))
        else:
            return make_response(jsonify(code=402, message=u"对不起，您未登录!"))


if __name__ == '__main__':
    scheduler.start()
    custom_log.log_info_task("Running on http://0.0.0.0:10000/ (Press CTRL+C to quit)")
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False, threaded=True)
