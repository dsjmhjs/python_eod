
# -*- coding: utf-8 -*-
import sys
import xml.etree.cElementTree as ET
from eod_aps.model.schema_common import Instrument
from eod_aps.model.eod_const import CustomEnumUtils
from eod_aps.server_python import *

date_utils = DateUtils()

Instrument_Type_Enum = const.INSTRUMENT_TYPE_ENUMS
Exchange_Type_Enum = const.EXCHANGE_TYPE_ENUMS
Market_Sector_Type_Enum = const.MARKETSECTOR_TYPE_ENUMS

custom_enum_utils = CustomEnumUtils()
SecruityIDSource_xml_dict = custom_enum_utils.enum(CS='102')


class XmlModel(object):
    """
       XmlModel
    """
    def __init__(self):
        pass


def __read_securities_xml():
    try:
        tree = ET.parse("Z:/dailyjob/daily_files/ETF/securities_20171220.xml")  # 打开xml文档
        # root = ET.fromstring(country_string) #从字符串传递xml
        root = tree.getroot()  # 获得root节点
    except Exception, e:
        print "Error:cannot parse file:securities_20171219.xml."
        sys.exit(1)

    global securities_xml_dict
    securities_xml_dict = dict()
    for child in root:
        securities_xml = XmlModel()
        for sub_child in child:
            if sub_child.tag in ['StockParams', 'FundParams', 'BondParams', 'WarrantParams', 'RepoParams',
                                 'OptionParams', 'PreferredStockParams']:
                stock_params_dict = dict()
                for sub_params in sub_child:
                    dict_key = sub_params.tag
                    stock_params_dict[dict_key] = sub_child.find(dict_key).text
                setattr(securities_xml, sub_child.tag, stock_params_dict)
            else:
                setattr(securities_xml, sub_child.tag, child.find(sub_child.tag).text)
        securities_xml_dict[securities_xml.SecurityID] = securities_xml

    # element = child.find('SecurityID')
    # if element is None:
    #     print "element:%s not found" % 'SecurityID'

    # for country in root.findall('Security'):  # 找到root节点下的所有country节点
    #     rank = country.find('SecurityID').text  # 子节点下节点rank的值
    #     name = country.find('EnglishName').text  # 子节点下属性name的值
    #     print name, rank


def __read_cashauctionparams_xml():
    try:
        tree = ET.parse("Z:/dailyjob/daily_files/ETF/cashauctionparams_20171220.xml")
        root = tree.getroot()
    except Exception, e:
        print "Error:cannot parse file:cashauctionparams_20171219.xml."
        sys.exit(1)

    global cashauctionparams_xml_dict
    cashauctionparams_xml_dict = dict()
    for child in root:
        cashauctionparams_xml = XmlModel()
        for sub_child in child:
            if sub_child.tag in ['Setting', ]:
                params_dict = dict()
                for sub_params in sub_child:
                    dict_key = sub_params.tag
                    params_dict[dict_key] = sub_child.find(dict_key).text
                setattr(cashauctionparams_xml, sub_child.tag, params_dict)
            else:
                setattr(cashauctionparams_xml, sub_child.tag, child.find(sub_child.tag).text)
        cashauctionparams_xml_dict[cashauctionparams_xml.SecurityID] = cashauctionparams_xml


def __read_negotiationparams_xml():
    try:
        tree = ET.parse("Z:/dailyjob/daily_files/ETF/negotiationparams_20171220.xml")
        root = tree.getroot()
    except Exception, e:
        print "Error:cannot parse file:cashauctionparams_20171219.xml."
        sys.exit(1)

    global negotiationparams_xml_dict
    negotiationparams_xml_dict = dict()
    for child in root:
        negotiationparams_xml = XmlModel()
        for sub_child in child:
            setattr(negotiationparams_xml, sub_child.tag, child.find(sub_child.tag).text)
        negotiationparams_xml_dict[negotiationparams_xml.SecurityID] = negotiationparams_xml


def __convert_to_instrument_update():
    stock_db_list = []
    for (SecurityID, securities_xml_info) in securities_xml_dict.items():
        if securities_xml_info.SecurityType not in ('1', '2', '3'):
            continue


def __convert_to_instrument():
    stock_db_list = []
    for (SecurityID, securities_xml_info) in securities_xml_dict.items():
        if securities_xml_info.SecurityType not in ('1', '2', '3'):
            continue

        stock_db = Instrument()
        stock_db.ticker = SecurityID.strip()
        if securities_xml_info.SecurityIDSource == SecruityIDSource_xml_dict.CS:
            stock_db.exchange_id = Exchange_Type_Enum.CS

        stock_db.name = securities_xml_info.EnglishName
        stock_db.ticker_exch = stock_db.ticker
        stock_db.ticker_exch_real = stock_db.ticker
        stock_db.market_status_id = 2
        stock_db.market_sector_id = Market_Sector_Type_Enum.Equity
        stock_db.type_id = Instrument_Type_Enum.CommonStock
        stock_db.ticker_isin = securities_xml_info.ISIN
        stock_db.crncy = 'CNY'

        cashauctionparams_xml_info = cashauctionparams_xml_dict[SecurityID]
        stock_db.round_lot_size = cashauctionparams_xml_info.BuyQtyUnit
        stock_db.tick_size_table = '0:%s' % float(cashauctionparams_xml_info.PriceTick)

        stock_db.fut_val_pt = 1 # ?
        stock_db.max_market_order_vol = 0
        stock_db.min_market_order_vol = 0
        stock_db.max_limit_order_vol = 1000000
        stock_db.min_limit_order_vol = 100

        stock_db.longmarginratio = 0
        stock_db.shortmarginratio = 999
        stock_db.longmarginratio_speculation = stock_db.longmarginratio
        stock_db.shortmarginratio_speculation = stock_db.shortmarginratio
        stock_db.longmarginratio_hedge = stock_db.longmarginratio
        stock_db.shortmarginratio_hedge = stock_db.shortmarginratio
        stock_db.longmarginratio_arbitrage = stock_db.longmarginratio
        stock_db.shortmarginratio_arbitrage = stock_db.shortmarginratio
        stock_db.multiplier = 1

        stock_db.is_settle_instantly = 0
        stock_db.is_purchase_to_redemption_instantly = 0
        stock_db.is_buy_to_redpur_instantly = 1
        stock_db.is_redpur_to_sell_instantly = 1

        negotiationparams_xml = negotiationparams_xml_dict[SecurityID]
        stock_db.prev_close = securities_xml_info.PrevClosePx
        stock_db.prev_settlementprice = stock_db.prev_close
        stock_db.uplimit = negotiationparams_xml.PriceUpperLimit
        stock_db.downlimit = negotiationparams_xml.PriceLowerLimit
        stock_db.prev_close_update_time = date_utils.get_now()
        stock_db.update_date = date_utils.get_now()
        stock_db_list.append(stock_db)

    server_host = server_constant.get_server_model('host')
    session_test = server_host.get_db_session('test')
    for instrument_db in stock_db_list:
        session_test.merge(instrument_db)
    session_test.commit()
    server_host.close()


def update_by_xml():
    __read_securities_xml()
    __read_cashauctionparams_xml()
    __read_negotiationparams_xml()
    __convert_to_instrument()


if __name__ == '__main__':
    update_by_xml()