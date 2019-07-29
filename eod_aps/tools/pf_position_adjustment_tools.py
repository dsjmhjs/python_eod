# -*- coding: utf-8 -*-
from eod_aps.model.schema_portfolio import PfAccount, PfPosition
from eod_aps.model.server_constans import server_constant
from eod_aps.tools.date_utils import DateUtils

date_utils = DateUtils()


def rounding_number(number_input):
    # 对股数进行四舍五入， 160--》200
    return int(round(float(number_input) / float(100), 0) * 100)


class PfPositionAdjustment(object):
    def __init__(self, server_name):
        self.__server_name = server_name
        self.__date_str = date_utils.get_next_trading_day()
        # self.__date_str = date_utils.get_today_str('%Y-%m-%d')

    def position_split_index(self):
        server_model = server_constant.get_server_model(self.__server_name)
        session_portfolio = server_model.get_db_session('portfolio')
        pf_account_dict = {x.id: x.fund_name for x in session_portfolio.query(PfAccount).filter(PfAccount.group_name == 'MultiFactor')}
        inversion_pf_account_dict = {value: key for (key, value) in pf_account_dict.items()}

        pf_position_list = []
        for x in session_portfolio.query(PfPosition).filter(PfPosition.date == self.__date_str):
            if x.id not in pf_account_dict:
                continue

            strategy_name = pf_account_dict[x.id]
            target_strategy_name = strategy_name.replace('ANN6031A', 'ANN6031B').replace('ANN6061A', 'ANN6061B')
            if strategy_name == target_strategy_name:
                print strategy_name
                continue

            if x.long > 0:
                # 处理股票
                qty = rounding_number(x.long / 2)
                # 仓位只有100的情况
                if x.long == qty or qty == 0:
                    print strategy_name, x.symbol, x.long
                    continue
                target_account_id = inversion_pf_account_dict[target_strategy_name]

                target_pf_position = PfPosition()
                target_pf_position.date = self.__date_str
                target_pf_position.id = target_account_id
                target_pf_position.symbol = x.symbol
                target_pf_position.hedgeflag = x.hedgeflag
                target_pf_position.long = qty
                target_pf_position.long_cost = (qty / x.long) * x.long_cost
                target_pf_position.long_avail = target_pf_position.long
                target_pf_position.yd_position_long = target_pf_position.long
                target_pf_position.yd_long_remain = target_pf_position.long
                target_pf_position.short = 0
                target_pf_position.short_cost = 0
                target_pf_position.short_avail = 0
                target_pf_position.yd_position_short = 0
                target_pf_position.yd_short_remain = 0
                pf_position_list.append(target_pf_position)

                x.long -= qty
                x.long_cost = x.long_cost - target_pf_position.long_cost
                x.long_avail = x.long
                x.yd_position_long = x.long
                x.yd_long_remain = x.long
                pf_position_list.append(x)
            elif x.short > 0:
                # 处理期货
                qty = int(x.short / 2)
                if x.short == qty or qty == 0:
                    print strategy_name, x.symbol, x.short
                    continue
                target_account_id = inversion_pf_account_dict[target_strategy_name]

                target_pf_position = PfPosition()
                target_pf_position.date = self.__date_str
                target_pf_position.id = target_account_id
                target_pf_position.symbol = x.symbol
                target_pf_position.hedgeflag = x.hedgeflag
                target_pf_position.long = 0
                target_pf_position.long_cost = 0
                target_pf_position.long_avail = 0
                target_pf_position.yd_position_long = 0
                target_pf_position.yd_long_remain = 0
                target_pf_position.short = qty
                target_pf_position.short_cost = (qty / x.short) * x.short_cost
                target_pf_position.short_avail = target_pf_position.short
                target_pf_position.yd_position_short = target_pf_position.short
                target_pf_position.yd_short_remain = target_pf_position.short
                pf_position_list.append(target_pf_position)

                x.short -= qty
                x.short_cost = x.short_cost - target_pf_position.short_cost
                x.short_avail = x.short
                x.yd_position_short = x.short
                x.yd_short_remain = x.short
                pf_position_list.append(x)

        for pf_position_db in pf_position_list:
            session_portfolio.merge(pf_position_db)
        session_portfolio.commit()


if __name__ == '__main__':
    pf_position_adjustment = PfPositionAdjustment('guangfa')
    pf_position_adjustment.position_split_index()
