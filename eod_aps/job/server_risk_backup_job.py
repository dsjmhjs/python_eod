# coding=utf-8
import pandas as pd
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.tools.common_utils import CommonUtils
from eod_aps.tools.tradeplat_position_tools import *
from eod_aps.model.schema_history import ServerRisk
from eod_aps.job import *


common_utils = CommonUtils()


def server_risk_backup_job():
    global server_host
    server_host = server_constant.get_server_model('host')

    __server_risk_backup_job('aggregator')

    server_list = server_constant.get_all_trade_servers()
    __validate_server_risk(server_list)
    server_host.close()


def __validate_server_risk(server_list):
    filter_date_str = date_utils.get_today_str('%Y-%m-%d')
    session_history = server_host.get_db_session('history')

    query_sql = "select server_name from history.server_risk where date = '%s' group by server_name" % filter_date_str

    server_risk_query = session_history.execute(query_sql)
    validate_list = []
    for server_risk_item in server_risk_query:
        validate_list.append(server_risk_item[0])

    server_list.sort()
    validate_list.sort()
    if len(server_list) != len(validate_list):
        error_message = 'Target Servers:%s\nValidate Servers:%s' % (','.join(server_list), ','.join(validate_list))
        email_utils2.send_email_group_all('[Error]Server Risk BackUp_%s' % filter_date_str, error_message)


def server_risk_validate_job(server_list):
    error_message_list = []
    for server_name in server_list:
        error_fund_name_list = __server_risk_validate(server_name)
        if len(error_fund_name_list) > 0:
            error_message_list.append('Pf_Account has position but not in GUI.\nServer:%s,Error Pf_Account List:%s' %
                                      (server_name, ','.join(error_fund_name_list)))
            email_utils2.send_email_group_all('[Error]Pf_Position Error', '\n'.join(error_message_list))


def __server_risk_validate(server_name):
    risk_account_list = []
    socket = socket_init(server_name)
    risk_msg_list = send_position_risk_request_msg(socket)
    for risk_item in risk_msg_list:
        account_name = risk_item.Key
        risk_account_list.append(account_name.lower())

    date_filter_str = date_utils.get_today_str('%Y-%m-%d')
    pf_account_dict = dict()
    server_model = server_constant.get_server_model(server_name)
    session_portfolio = server_model.get_db_session('portfolio')
    for pf_account_db in session_portfolio.query(PfAccount):
        pf_account_dict[pf_account_db.id] = pf_account_db.fund_name

    error_fund_name_list = []
    for pf_account_item in session_portfolio.query(PfPosition.id).filter(PfPosition.date == date_filter_str).group_by(PfPosition.id):
        if pf_account_item[0] not in pf_account_dict:
            continue
        fund_name = pf_account_dict[pf_account_item[0]]
        if fund_name.lower() not in risk_account_list:
            error_fund_name_list.append(fund_name)
    return error_fund_name_list


def __server_risk_backup_job(server_name):
    socket = socket_init(server_name)
    send_login_msg(socket)
    trade_server_info_msg = send_tradeserverinfo_request_msg(socket)
    trade_server_info_list = []
    for trade_server_info in trade_server_info_msg.TradeServerInfo:
        trade_server_info_list.append(trade_server_info)
    send_subscribetradeserverinfo_request_msg(socket, trade_server_info_list)

    instrument_dict, market_dict = send_instrument_info_request_msg(socket)
    instrument_view_dict = dict()
    instrument_symbol_dict = dict()
    for (key, instrument_msg) in instrument_dict.items():
        market_msg = market_dict[key]
        instrument_view = InstrumentView(instrument_msg, market_msg)
        instrument_view_dict[key] = instrument_view
        instrument_symbol_dict[instrument_view.Ticker] = instrument_view
    for (key, instrument_view) in instrument_view_dict.items():
        if len(instrument_view.UnderlyingTickers) > 0:
            for underlying_ticker in instrument_view.UnderlyingTickers:
                if underlying_ticker.split(' ')[0] not in instrument_symbol_dict:
                    continue
                instrument_view.Underlyings.append(instrument_symbol_dict[underlying_ticker.split(' ')[0]])

    risk_msg_list = send_position_risk_request_msg(socket)
    risk_view_list = []
    for risk_item in risk_msg_list:
        account_name = risk_item.Key
        for position_item in risk_item.Value:
            instrument_view = instrument_view_dict[position_item.Key]
            risk_view = RiskView(instrument_view, position_item.Value, account_name)

            if risk_view.Ticker.isdigit():
                future_flag = 0
            else:
                future_flag = 1

            risk_view_list.append([risk_view.AccountName, future_flag, risk_view.fee, risk_view.trading_pl,
                                   risk_view.position_pl, risk_view.total_pl, risk_view.total_stocks_value,
                                   risk_view.total_future_value, risk_view.delta, risk_view.gamma, risk_view.vega,
                                   risk_view.theta, risk_view.total_bought_value, risk_view.total_sold_value])

    if len(risk_view_list) == 0:
        return

    risk_view_df = pd.DataFrame(risk_view_list, columns=["AccountName", "Future_Flag", "Fee", "Trading_PL",
                                                         "Position_PL", "Total_PL", "Total_Stocks_Value",
                                                         "Total_Future_Value", "Delta", "Gamma", "Vega", "Theta",
                                                         "Total_Bought_Value", "Total_Sold_Value"])
    groupby_df1 = risk_view_df.groupby(["AccountName", "Future_Flag"]).sum()["Total_PL"]

    pf_account_pl_dict = dict()
    for (account_name, future_flag), total_pl in groupby_df1.to_dict().items():
        if future_flag == 0:
            if account_name in pf_account_pl_dict:
                temp_pl_list = pf_account_pl_dict[account_name]
                temp_pl_list = [total_pl, temp_pl_list[1]]
            else:
                temp_pl_list = [total_pl, 0]
        elif future_flag == 1:
            if account_name in pf_account_pl_dict:
                temp_pl_list = pf_account_pl_dict[account_name]
                temp_pl_list = [temp_pl_list[0], total_pl]
            else:
                temp_pl_list = [0, total_pl]
        pf_account_pl_dict[account_name] = temp_pl_list
    groupby_df2 = risk_view_df.groupby("AccountName").sum()[["Fee", "Trading_PL", "Position_PL", "Total_PL",
                                                             "Total_Stocks_Value", "Total_Future_Value",
                                                             "Delta", "Gamma", "Vega", "Theta", "Total_Bought_Value",
                                                             "Total_Sold_Value"]]
    risk_dict = groupby_df2.to_dict("index")

    today_str = date_utils.get_today_str('%Y-%m-%d')
    session_history = server_host.get_db_session('history')
    session_history.query(ServerRisk).filter(ServerRisk.date == today_str).delete()
    for (full_strategy_name, risk_value_dict) in risk_dict.items():
        strategy_name_items = full_strategy_name.split('@')
        server_risk = ServerRisk()
        server_risk.server_name = common_utils.get_server_name(strategy_name_items[1])
        server_risk.date = today_str
        server_risk.strategy_name = strategy_name_items[0]
        server_risk.position_pl = risk_value_dict['Position_PL']
        server_risk.trading_pl = risk_value_dict['Trading_PL']
        server_risk.fee = risk_value_dict['Fee']
        server_risk.total_pl = risk_value_dict['Total_PL']
        server_risk.total_stocks_value = risk_value_dict['Total_Stocks_Value']
        server_risk.total_future_value = risk_value_dict['Total_Future_Value']
        server_risk.delta = risk_value_dict['Delta']
        server_risk.gamma = risk_value_dict['Gamma']
        server_risk.vega = risk_value_dict['Vega']
        server_risk.theta = risk_value_dict['Theta']
        server_risk.total_bought_value = risk_value_dict['Total_Bought_Value']
        server_risk.total_sold_value = risk_value_dict['Total_Sold_Value']

        pf_account_pl_item = pf_account_pl_dict[full_strategy_name]
        server_risk.stocks_pl = pf_account_pl_item[0]
        server_risk.future_pl = pf_account_pl_item[1]
        session_history.merge(server_risk)
    session_history.commit()


if __name__ == '__main__':
    # server_risk_backup_job('guoxin')
    # all_trade_servers_list = server_constant.get_all_trade_servers()
    # all_trade_servers_list.remove('guangfa')
    server_risk_backup_job()
