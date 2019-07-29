# -*- coding: utf-8 -*-
from eod_aps.model.instrument import Instrument
from eod_aps.model.pf_account import PfAccount
from eod_aps.model.pf_position import PfPosition
from eod_aps.model.server_constans import ServerConstant
from eod_aps.model.trade2 import Trade2
from eod_aps.tools.date_utils import DateUtils
from itertools import islice
from eod_aps.tools.email_utils import EmailUtils

date_utils = DateUtils()
sum_pl_dict = dict()
strategy_pnl_dict = dict()
date_set = set()
attachment_file_list = []

email_utils = EmailUtils(EmailUtils.group1)

def __build_pf_position_dict(server_model, pf_account_db):
    pf_position_dict = dict()

    session_portfolio = server_model.get_db_session('portfolio')
    query_pf_position = session_portfolio.query(PfPosition)
    for pf_position_db in query_pf_position.filter(PfPosition.id == str(pf_account_db.id)):
        date_str = pf_position_db.date.strftime("%Y-%m-%d")
        if date_str in pf_position_dict:
            pf_position_dict[date_str].append(pf_position_db)
        else:
            pf_position_dict[date_str] = [pf_position_db]
    return pf_position_dict


def __build_trade_dict(server_model, pf_account_db):
    trade_dict = dict()

    session_om = server_model.get_db_session('om')
    trade_list = session_om.query(Trade2)
    strategy_id = '%s.%s' % (pf_account_db.group_name, pf_account_db.name)
    for trade_db in trade_list.filter(Trade2.strategy_id == strategy_id):
        date_str = trade_db.time.strftime("%Y-%m-%d")
        if date_str in trade_dict:
            trade_dict[date_str].append(trade_db)
        else:
            trade_dict[date_str] = [trade_db]
    return trade_dict


def server_enter(server_model, pf_account_db):
    session_portfolio = server_model.get_db_session('portfolio')
    instrument_db_dict = __build_ticker_exchange(server_model)
    pf_position_dict = __build_pf_position_dict(server_model, pf_account_db)
    trade_dict = __build_trade_dict(server_model, pf_account_db)

    date_info = session_portfolio.execute('select min(date),max(date) from portfolio.pf_position t where t.ID=' \
                                          + str(pf_account_db.id)).first()
    start_date, end_date = date_info[0], date_info[1]
    date_list = date_utils.get_trading_day_list(start_date, end_date)[:-2]

    future_open_dict = dict()
    prev_equity_total = 0.0

    report_result = []
    for i in range(0, len(date_list)):
        date_set.add(date_list[i])
        date_str = date_list[i].strftime("%Y-%m-%d")
        stock_file_dict = __read_stock_daily_file(date_str)
        future_file_dict = __read_future_daily_file(date_str)

        pf_position_list = pf_position_dict[date_str]

        total_buy_money = 0.0
        total_sell_money = 0.0
        if date_str in trade_dict:
            trade_list = trade_dict[date_str]
            for trade_db in trade_list:
                ticker = trade_db.symbol.split(' ')[0]
                instrument_db = instrument_db_dict[ticker]
                if trade_db.trade_type == 0:
                    if trade_db.qty > 0:
                        total_buy_money += float(trade_db.price) * abs(trade_db.qty) * (1+0.00025)
                    else:
                        total_sell_money += float(trade_db.price) * abs(trade_db.qty) * (1 - 0.00125)
                elif trade_db.trade_type == 2:
                    total_buy_money += float(trade_db.price) * abs(trade_db.qty) * float(instrument_db.fut_val_pt) * (0.5 + 0.000026)
                    future_open_dict[ticker] = trade_db
                elif trade_db.trade_type == 3:
                    future_open_trade = future_open_dict[ticker]
                    total_sell_money += float(future_open_trade.price) * abs(future_open_trade.qty) \
* float(instrument_db.fut_val_pt) * 0.5 + (float(future_open_trade.price) - float(trade_db.price)) * abs(trade_db.qty) *\
float(instrument_db.fut_val_pt) * (1 - 0.000026)
            total_money_change = total_sell_money -total_buy_money
        else:
            total_money_change = 0.0

        equity_total = 0.0
        for pf_position_db in pf_position_list:
            instrument_db = instrument_db_dict[pf_position_db.symbol]
            if instrument_db.type_id == 4:
                ticker_close_price = float(stock_file_dict[pf_position_db.symbol].split(',')[7])
                equity_total += float(pf_position_db.long) * ticker_close_price
            elif instrument_db.type_id == 1:
                future_open_trade = future_open_dict[instrument_db.ticker]
                future_close_price = float(future_file_dict[pf_position_db.symbol].split(',')[10])
                equity_total += float(pf_position_db.short) * float(future_open_trade.price) * float(instrument_db.fut_val_pt) * 0.5\
                    + (float(future_open_trade.price) - float(future_close_price)) * float(pf_position_db.short) * float(instrument_db.fut_val_pt)

        if prev_equity_total == 0:
            pnl = equity_total + total_money_change
            equity_base = abs(total_money_change)
        else:
            pnl = equity_total + total_money_change - prev_equity_total
            if pnl > 0:
                equity_base = prev_equity_total
            else:
                equity_base = prev_equity_total + abs(total_money_change)
        return_rate = pnl * 100 / equity_base

        report_result.append('%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f%%' \
              % (date_str, total_buy_money, total_sell_money, total_money_change, prev_equity_total, equity_total, pnl, equity_base, return_rate))

        sum_pl_dict['%s|%s' % (date_str, 'return_rate_%s' % pf_account_db.name)] = return_rate

        strategy_pnl_key = '%s|%s' % (date_str, pf_account_db.name)
        strategy_pnl_dict[strategy_pnl_key] = '%s|%s' % (pnl, equity_base)

        prev_equity_total = equity_total

    file_path = 'E:/dailyFiles/strategy_report_%s.csv' % pf_account_db.name
    report_result.insert(0, 'date,total_buy_money,total_sell_money,total_money_change,prev_equity_total,equity_total,pnl,equity_base,return_rate')
    file_object = open(file_path, 'w+')
    file_object.write('\n'.join(report_result))
    file_object.close()
    attachment_file_list.append(file_path)


def __read_stock_daily_file(date_str):
    base_file_folder = 'Z:/data/factor/pre_close'
    file_path = '%s/%s.csv' % (base_file_folder, date_str.replace('-', ''))

    stock_file_dict = dict()
    input_file = open(file_path)
    for line in islice(input_file, 1, None):
        ticker = line.split(',')[0].split('.')[0]
        stock_file_dict[ticker] = line
    return stock_file_dict


def __read_future_daily_file(date_str):
    file_path = 'Z:/data/future/ctp_update/%s/tick/summary.csv' % date_str.replace('-', '')

    future_file_dict = dict()
    input_file = open(file_path)
    for line in islice(input_file, 1, None):
        ticker = line.split(',')[0]
        future_file_dict[ticker] = line
    return future_file_dict


def __build_ticker_exchange(server_model):
    instrument_dict = dict()
    session_history= server_model.get_db_session('history')
    query = session_history.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id.in_((1, 4))):
        instrument_dict[instrument_db.ticker] = instrument_db
    return instrument_dict


def __email_algo_pf_position(pf_account_db_list):
    date_list = list(date_set)
    date_list.sort()

    email_list = []
    for strategy_base_name in ['Long_IndNorm', 'Long_MV10Norm']:
        email_list.append('<table border="1"><tr><th>Pf_Account</th>')
        for date_str in date_list:
            email_list.append('<th>%s</th>' % date_str)
        email_list.append('<th>Cum_Ret</th></tr>')

        for pf_account_db in pf_account_db_list:
            if strategy_base_name not in pf_account_db.name:
                continue
            email_list.append('<tr><td>%s</td>' % pf_account_db.name)

            total_value = 1
            for date_str in date_list:
                key = '%s|%s' % (date_str, 'return_rate_%s' % pf_account_db.name)
                if key in sum_pl_dict:
                    email_list.append('<td>%.3f%%</td>' % sum_pl_dict[key])
                    total_value *= 1 + sum_pl_dict[key] / 100
                else:
                    email_list.append('<td></td>')
            total_value -= 1
            email_list.append('<td>%.3f%%</td>' % (total_value * 100))
            email_list.append('</tr>')

        email_list.append('<tr><td>%s</td>' % strategy_base_name)
        total_value = 1
        for date_str in date_list:
            sum_pnl = 0.0
            sum_equity_base = 0.0
            for pf_account_db in pf_account_db_list:
                if strategy_base_name not in pf_account_db.name:
                    continue
                strategy_pnl_key = '%s|%s' % (date_str, pf_account_db.name)
                if strategy_pnl_key in strategy_pnl_dict:
                    (pnl_temp, equity_base) = strategy_pnl_dict[strategy_pnl_key].split('|')
                    sum_pnl += float(pnl_temp)
                    sum_equity_base += float(equity_base)
            if sum_equity_base > 0:
                return_rate = sum_pnl * 100 / sum_equity_base
                email_list.append('<td>%.3f%%</td>' % return_rate)
                total_value *= 1 + return_rate / 100
            else:
                email_list.append('<td>0</td>')
        total_value -= 1
        email_list.append('<td>%.3f%%</td>' % (total_value * 100))
        email_list.append('</tr>')
        email_list.append('</table>')
    email_utils.send_email_path('Algo Pf_Position Info!', ''.join(email_list), ','.join(attachment_file_list), 'html')


def get_account_id_list(server_model):
    session_portfolio = server_model.get_db_session('portfolio')
    query_sql = "select id from portfolio.pf_account a where a.FUND_NAME like '%s' and a.id in \
(select ID from portfolio.pf_position t where t.DATE = '%s' group by t.ID)" % (
    '%Norm%', date_utils.get_today_str('%Y-%m-%d'))

    pf_account_db_list = []
    for account_id in session_portfolio.execute(query_sql):
        query_pf_account = session_portfolio.query(PfAccount)
        pf_account_db = query_pf_account.filter(PfAccount.id == str(account_id[0])).first()
        pf_account_db_list.append(pf_account_db)
    return pf_account_db_list


def start():
    server_model = ServerConstant().get_server_model('huabao')
    pf_account_db_list = get_account_id_list(server_model)

    for pf_account_db in pf_account_db_list:
        server_enter(server_model, pf_account_db)
    __email_algo_pf_position(pf_account_db_list)


if __name__ == '__main__':
    start()
