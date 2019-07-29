# -*- coding: utf-8 -*-
# 通过wind的api接口导出数据
import pandas as pd
from WindPy import *
from eod_aps.model.instrument import Instrument
from eod_aps.job import *

reload(sys)
sys.setdefaultencoding('utf8')

stock_list = []
i = 1

def __wind_login():
    w.start()


def __wind_export_date(columes, start_date, end_date):
    global i
    starttime = datetime.now()
    dataframe_all = pd.DataFrame()
    for ticker in stock_list:
        wind_data = w.wsd(ticker, columes, start_date, end_date, "Period=D")
        ret = pd.DataFrame(data=wind_data.Data, index=wind_data.Fields, columns=wind_data.Times).T
        ret['ticker'] = pd.Series(ticker, index=ret.index)
        dataframe_all = pd.concat([dataframe_all, ret])
    dataframe_all.to_csv('wind_export_file_' + str(i) + '.csv')
    endtime = datetime.now()
    i += 1


def __query_stock_list():
    server_model = server_constant.get_server_model('host')
    session = server_model.get_db_session('common')
    query = session.query(Instrument)
    for instrument_db in query.filter(Instrument.type_id == 4):
        if instrument_db.exchange_id == 18:
            stock_list.append(instrument_db.ticker + '.SH')
        elif instrument_db.exchange_id == 19:
            stock_list.append(instrument_db.ticker + '.SZ')
    server_model.close()


def wind_export_job():
    __query_stock_list()
    __wind_login()

#     columes = "trade_status,share_liqa_pct,open,high,low,close,vwap,chg,pct_chg,turn,free_turn,volume,amt,swing,\
# susp_days,susp_reason,lastradeday_s,maxupordown,mrg_long_amt,margin_salerepayamount,mrg_long_bal,mrg_short_vol,\
# mrg_short_vol_repay,margin_saletradingamount,margin_salerepayamount,mrg_short_bal,mf_amt,mf_amt_close,mf_amt_open,\
# mf_amt_ratio,mv_vol_ratio,mkt_cap_ard,pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,pcf_ocf_ttm,pcf_ncf_ttm,\
# pcf_ocflyr,pcf_nflyr,dividendyield,dividendyield2,estpe_FY1,estpe_FY2,estpe_FY3,pe_est_ftm,estpeg_FY1,estpeg_FY2,\
# estpb_FY1,estpb_FY2,estpb_FY3,val_evtoebitda2,mkt_freeshares,mkt_cap_CSRC,mkt_cap_ashare2,mkt_cap_ashare,beta_100w,\
# beta_24m,betadf,west_eps_FY1,west_eps_FY2,west_eps_FY3,wrating_targetprice,rating_avg"

    columes_1 = "trade_status,share_liqa_pct,open,high,low,close,vwap,chg,pct_chg,turn,free_turn,volume,amt,swing,\
susp_days,susp_reason,lastradeday_s,maxupordown,mrg_long_amt,margin_salerepayamount,mrg_long_bal,mrg_short_vol"

    columes_2 = "mrg_short_vol_repay,margin_saletradingamount,margin_salerepayamount,mrg_short_bal,mf_amt,mf_amt_close,mf_amt_open,\
mf_amt_ratio,mv_vol_ratio,mkt_cap_ard,pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,pcf_ocf_ttm,pcf_ncf_ttm"

    columes_3 = "pcf_ocflyr,pcf_nflyr,dividendyield,dividendyield2,estpe_FY1,estpe_FY2,estpe_FY3,pe_est_ftm,estpeg_FY1,estpeg_FY2,\
estpb_FY1,estpb_FY2,estpb_FY3,val_evtoebitda2,mkt_freeshares,mkt_cap_CSRC,mkt_cap_ashare2,mkt_cap_ashare,beta_100w"

    columes_4 = "beta_24m,betadf,west_eps_FY1,west_eps_FY2,west_eps_FY3,wrating_targetprice,rating_avg"

    start_date = '2016-06-21'
    end_date = '2016-06-21'
    __wind_export_date(columes_2, start_date, end_date)


if __name__ == '__main__':
    wind_export_job()
