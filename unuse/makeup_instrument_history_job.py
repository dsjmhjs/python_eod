# -*- coding: utf-8 -*-
# 根据common.instrument补齐history.instrument_history表中缺失的数据
from eod_aps.model.instrument import Instrument
from eod_aps.job import *


def makeup_instrument_history_job(server_name):
    server_model = server_constant.get_server_model(server_name)
    session_common = server_model.get_db_session('common')

    instrument_history_dict = dict()
    trading_time_dict = dict()
    query = session_common.query(Instrument)
    for instrument_history_db in query.filter(Instrument.type_id == 1):
        instrument_history_dict[instrument_history_db.ticker] = '0'
        trading_time_dict[instrument_history_db.undl_tickers] = instrument_history_db

    query = session_common.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 1):
        if instrument_db.exchange_id == 22 and len(instrument_db.ticker) == 5:
            ticker = instrument_db.ticker[:2] + '1' + instrument_db.ticker[2:]
        else:
            ticker = instrument_db.ticker

        if not ticker in instrument_history_dict:
            task_logger.info('prepar update ticker:%s' % ticker)
            instrument_history_new = InstrumentHistory()
            instrument_history = trading_time_dict[instrument_db.undl_tickers]

            if instrument_db.exchange_id == 22 and len(instrument_db.ticker) == 5:
                ticker = instrument_db.ticker[:2] + '1' + instrument_db.ticker[2:]
            else:
                ticker = instrument_db.ticker

            instrument_history_new.ticker = ticker
            instrument_history_new.exchange_id = instrument_history.exchange_id
            instrument_history_new.type_id = 1
            instrument_history_new.effective_since = instrument_history.effective_since
            instrument_history_new.expire_date = instrument_history.expire_date
            instrument_history_new.thours = instrument_history.thours
            instrument_history_new.ticker_wind = ticker + '.' + instrument_history.ticker_wind.split('.')[1]
            instrument_history_new.market_sector_id = instrument_history.market_sector_id
            instrument_history_new.name = ticker
            instrument_history_new.market_status_id = instrument_history.market_status_id
            instrument_history_new.round_lot_size = instrument_history.round_lot_size
            instrument_history_new.tick_size_table = instrument_history.tick_size_table
            instrument_history_new.fut_val_pt = instrument_history.fut_val_pt
            instrument_history_new.ticker_exch = ticker
            instrument_history_new.ticker_exch_real = ticker
            instrument_history_new.crncy = 'CNY'
            instrument_history_new.undl_tickers = instrument_history.undl_tickers
            instrument_history_new.max_market_order_vol = instrument_history.max_market_order_vol
            instrument_history_new.min_market_order_vol = instrument_history.min_market_order_vol
            instrument_history_new.max_limit_order_vol = instrument_history.max_limit_order_vol
            instrument_history_new.min_limit_order_vol = instrument_history.min_limit_order_vol
            instrument_history_new.longmarginratio = instrument_history.longmarginratio
            instrument_history_new.shortmarginratio = instrument_history.shortmarginratio
            instrument_history_new.multiplier = instrument_history.multiplier
            instrument_history_new.is_settle_instantly = instrument_history.is_settle_instantly
            instrument_history_new.is_purchase_to_redemption_instantly = instrument_history.is_purchase_to_redemption_instantly
            instrument_history_new.is_buy_to_redpur_instantly = instrument_history.is_buy_to_redpur_instantly
            instrument_history_new.is_redpur_to_sell_instantly = instrument_history.is_redpur_to_sell_instantly
            session_history.add(instrument_history_new)
    session_common.commit()
    session_history.commit()


if __name__ == '__main__':
    makeup_instrument_history_job('nanhua')
