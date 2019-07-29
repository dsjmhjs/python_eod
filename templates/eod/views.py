# coding: utf-8
from eod_aps.model.schema_portfolio import AccountTradeRestrictions
from eod_aps.model.schema_common import FutureMainContract
from eod_aps.model.schema_history import HolidayInfo
from eod_aps.model.schema_jobs import OptionTrade, StrategyIntradayParameter

from . import eod
from flask import request, jsonify, make_response
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import CustomEnumUtils, const

custom_enum_utils = CustomEnumUtils()
exchange_type_inversion_dict = custom_enum_utils.enum_to_dict(const.EXCHANGE_TYPE_ENUMS, True)


@eod.route('/query_main_symbol', methods=['GET', 'POST'])
def query_main_symbol():
    main_symbol_list = []
    server_model = server_constant.get_server_model('host')
    session_common = server_model.get_db_session('common')
    for main_symbol_item in session_common.query(FutureMainContract):
        main_symbol_dict = main_symbol_item.to_dict()
        main_symbol_dict['exchange'] = exchange_type_inversion_dict[main_symbol_item.exchange_id]
        main_symbol_list.append(main_symbol_dict)
    main_symbol_list.sort(key=lambda obj: obj['exchange'])
    return make_response(jsonify(code=200, data=main_symbol_list), 200)


@eod.route('/query_holiday_list', methods=['GET', 'POST'])
def query_holiday_list():
    params = request.json
    query_year = params.get('query_year')

    holiday_list = []
    server_model = server_constant.get_server_model('host')
    session_history = server_model.get_db_session('history')
    for holiday_item in session_history.query(HolidayInfo):
        holiday_str = holiday_item.holiday.strftime('%Y-%m-%d')
        if query_year not in holiday_str:
            continue
        holiday_dict = holiday_item.to_dict()
        holiday_dict['holiday'] = holiday_str
        holiday_list.append(holiday_dict)
    holiday_list.sort(key=lambda obj: obj['holiday'])
    return make_response(jsonify(code=200, data=holiday_list), 200)


@eod.route('/query_account_trade_restrictions', methods=['GET', 'POST'])
def query_account_trade_restrictions():
    query_params = request.json
    query_server_name = query_params.get('server_name')
    query_fund_name = query_params.get('fund_name')
    query_ticker = query_params.get('ticker')

    query_result_list = []
    server_account_dict = const.EOD_CONFIG_DICT['server_account_dict']
    for server_name in server_constant.get_trade_servers():
        if query_server_name and query_server_name != server_name:
            continue
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        for atr_item in session_portfolio.query(AccountTradeRestrictions):
            if query_ticker and query_ticker.lower() not in atr_item.ticker.lower():
                continue

            real_account_db = None
            for real_account_item in server_account_dict[server_name]:
                if atr_item.account_id == real_account_item.accountid:
                    real_account_db = real_account_item
                    break
            if query_fund_name and query_fund_name != real_account_db.fund_name:
                continue

            atr_dict = atr_item.to_dict()
            atr_dict['server_name'] = server_name
            atr_dict['fund_name'] = real_account_db.fund_name
            query_result_list.append(atr_dict)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    total_number = len(query_result_list)
    query_result = {'data': query_result_list[(query_page - 1) * query_size: query_page * query_size],
                    'total': total_number,
                    }
    return make_response(jsonify(code=200, data=query_result), 200)


@eod.route('/query_option_trade', methods=['GET', 'POST'])
def query_option_trade():
    query_params = request.json
    query_ticker = query_params.get('ticker')

    option_trade_list = []
    server_model = server_constant.get_server_model('host')
    session_jobs = server_model.get_db_session('jobs')
    for option_trade_db in session_jobs.query(OptionTrade):
        if query_ticker and query_ticker not in option_trade_db.ticker:
            continue
        option_trade_dict = option_trade_db.to_dict()
        option_trade_dict['date'] = option_trade_db.date.strftime('%Y-%m-%d')
        option_trade_list.append(option_trade_dict)

    query_page = int(query_params.get('page'))
    query_size = int(query_params.get('size'))
    result_list = option_trade_list[(query_page - 1) * query_size: query_page * query_size]
    query_result = {'data': result_list, 'total': len(option_trade_list)}
    return make_response(jsonify(code=200, data=query_result), 200)


@eod.route('/edit_option_trade', methods=['POST'])
def edit_option_trade():
    params = request.json
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    id = params.get('id')
    if id:
        option_trade_db = session_jobs.query(OptionTrade).filter(OptionTrade.id == id).first()
    else:
        option_trade_db = OptionTrade()

    option_trade_db.date = params.get('date'),
    option_trade_db.server_name = params.get('server_name'),
    option_trade_db.fund_name = params.get('fund_name'),
    option_trade_db.ticker = params.get('ticker'),
    option_trade_db.price = float(params.get('price')),
    option_trade_db.volume = params.get('volume'),
    option_trade_db.direction = params.get('direction'),
    session_jobs.merge(option_trade_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, data=u"保存成功"), 200)


@eod.route('/remove_option_trade', methods=['GET', 'POST'])
def remove_option_trade():
    params = request.json
    id = params.get('del_id')
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    option_trade_db = session_jobs.query(OptionTrade).filter(OptionTrade.id == id).first()
    session_jobs.delete(option_trade_db)
    session_jobs.commit()
    return make_response(jsonify(code=200, message=u"删除成功"), 200)


@eod.route('/query_strategy_intraday_parameter', methods=['GET', 'POST'])
def query_strategy_intraday_parameter():
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    data = []
    for obj in session_jobs.query(StrategyIntradayParameter):
        data.append({'strategy_name': obj.strategy_name, 'fund_name': obj.fund_name, 'parameter': obj.parameter,
                     'parameter_value': obj.parameter_value})
    return make_response(jsonify(code=200, data=data), 200)


@eod.route('/save_intraday_parameter', methods=['GET', 'POST'])
def save_intraday_parameter():
    params = request.json
    intraday_parameter_data = params.get('params')
    server_host = server_constant.get_server_model('host')
    session_jobs = server_host.get_db_session('jobs')
    for item in intraday_parameter_data:
        obj = StrategyIntradayParameter()
        obj.fund_name = item['fund_name']
        if obj.fund_name == '':
            return make_response(jsonify(code=100, message=u'fund_name不能为空'), 200)
        obj.parameter_value = item['parameter_value']
        obj.strategy_name = item['strategy_name']
        obj.parameter = item['parameter']
        session_jobs.merge(obj)
    # for item in add_parameter_data:
    #     add_obj = StrategyIntradayParameter()
    #     add_obj.fund_name = item['fund_name']
    #     add_obj.parameter_value = item['parameter_value']
    #     add_obj.strategy_name = item['strategy_name']
    #     add_obj.parameter = item['parameter']
    #     session_jobs.add(add_obj)
    session_jobs.commit()
    session_jobs.close()
    return make_response(jsonify(code=200, message=u"保存成功"), 200)
