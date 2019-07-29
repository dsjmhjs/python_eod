# -*- coding: utf-8 -*-
import time
import datetime
from flask import json, request, make_response, jsonify
from redis_bar import Bar, Line
from . import display_module
from eod_aps.tools.date_utils import DateUtils

date_utils = DateUtils()

temp_data_dict = dict()


# ============================= live show ======================================
@display_module.route('/latest_bar_report', methods=["GET", "POST"])
def latest_bar_report():
    config = request.json
    username = config['username']
    latest_bar_time_key = 'latest_bar_time|%s' % username
    latest_bar_data_key = 'latest_bar_data|%s' % username
    t = time.time()
    # if latest_bar_data_key not in temp_data_dict:
    if latest_bar_time_key in temp_data_dict:
        temp_t = t - temp_data_dict[latest_bar_time_key][0]
        if temp_t < 30:
            json_result = temp_data_dict[latest_bar_data_key]
            return make_response(
                jsonify(code=200, message=temp_data_dict[latest_bar_time_key][1], data=json_result))
    del config['username']
    send_data = Bar(config).get_bar_report()
    time_str = time.strftime('%H:%M:%S', time.localtime())
    json_result = json.dumps(send_data)
    temp_data_dict[latest_bar_data_key] = json_result
    temp_data_dict[latest_bar_time_key] = (t, time_str)
    return make_response(jsonify(code=200, message=time_str, data=json_result))
    # else:
    #     return make_response(
    #         jsonify(code=100, message=temp_data_dict[latest_bar_time_key][1], data=temp_data_dict[latest_bar_data_key]))


@display_module.route('/latest_line_report', methods=["GET", "POST"])
def latest_line_report():
    config = request.json
    username = config['username']
    latest_line_time_key = 'latest_line_time|%s' % username
    latest_line_data_key = 'latest_line_data|%s' % username
    t = time.time()
    # if latest_line_data_key not in temp_data_dict:
    if latest_line_time_key in temp_data_dict:
        temp_t = t - temp_data_dict[latest_line_time_key][0]
        if temp_t < 30:
            json_result = temp_data_dict[latest_line_data_key]
            return make_response(
                jsonify(code=200, message=temp_data_dict[latest_line_time_key][1], data=json_result))
    config = request.json
    time_now = config['datetime']
    time_slice = time_now.split(':')
    time_now = ':'.join(map(lambda x: x.zfill(2), time_slice))
    # line_object = LatestLine(config)
    # send_data = line_object.get_line_report(time_now)
    line = Line(config)
    time_str = time.strftime('%H:%M:%S', time.localtime())
    send_data = line.get_line_report()
    json_result = json.dumps(send_data)
    temp_data_dict[latest_line_data_key] = json_result
    temp_data_dict[latest_line_time_key] = (t, time_str)
    return make_response(jsonify(code=200, message=u"OK", data=json_result))
    # else:
    #     return make_response(
    #         jsonify(code=100, message=temp_data_dict[latest_line_time_key][1],
    #                 data=temp_data_dict[latest_line_data_key]))
