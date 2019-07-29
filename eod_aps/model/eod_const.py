# -*- coding: utf-8 -*-
from collections import OrderedDict


class _const(object):
    class ConstError(TypeError):
        pass

    def __setattr__(self, key, value):
        if key in self.__dict__:
            raise self.ConstError, "Changing const.%s" % key
        else:
            self.__dict__[key] = value

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.key
        else:
            return None


const = _const()

# IndexArb定义:Buy = 1, Sell = -1, Purchase = 2, Redemption = 3
const.DIRECTION_MAP = {'N': 'undefined', '0': '1', '1': '-1'}

# IndexArb定义:Normal = 0, Short = 1, Open = 2, Close = 3, CloseYesterday = 4, RedPur = 5, MergeSplit = 6,
# NA = 7, Exercise = 8, Merge = 9, Split = 10, Purchase = 11, Redeem = 12
const.TRADE_TYPE_MAP = {'0': '2', '1': '3'}

# IndexArb定义:None = -1,New = 0,PartialFilled = 1,Filled = 2,DoneForDay = 3,Canceled = 4,Replace = 5,
# PendingCancel = 6,Stopped = 7,Rejected = 8,Suspended = 9,PendingNew = 10,Calculated = 11,Expired = 12,
# AcceptedForBidding = 13,PendingReplace = 14,EndAsSucceed = 15,Accepted =16,InternalRejected = 17
const.ORDER_STATUS_MAP = {'0': '2', '1': '1', '2': '1', '3': '0', '4': '8', '5': '4', 'a': 'undefined', 'b': '0',
                          'c': '0'}

# IndexArb定义:投机 = 0, 套利 = 1, 套保 = 2
const.HEDGE_FLAG_MAP = {'1': '0', '2': '1', '3': '2', '4': '2'}

const.INSTRUMENT_TYPE_MAP = {"1": "Future", "10": "Option", "4": "CommonStock", "6": "Index", "7": "MutualFund",
                             "15": "MMF", "16": "StructuredFund"}

const.CUSTOMIZED_SERVICES_MAP = {}

const.HOLIDAYS = [
    '20130923', '20131008', '20140102', '20140207', '20140408',
    '20140505', '20140603', '20140909', '20141008', '20150105',
    '20150225', '20150407', '20150504', '20150623', '20150907',
    '20150928', '20151008', '20160104', '20160215', '20160405',
    '20160503', '20160613', '20160819', '20161010',
]

const.JOB_START_TIME_DICT = dict()
const.JOB_END_TIME_DICT = dict()

const.EOD_CONFIG_DICT = dict()

# 缓存服务器配置信息
const.CONFIG_SERVER_LIST = []


# ---------------------------enum-----------------------------------
class CustomEnumUtils(object):
    """
        自定义字典类
    """

    def __init__(self):
        pass

    @staticmethod
    def enum(**enums):
        return type('Enum', (), enums)

    @staticmethod
    def enum_to_dict(enum, inversion_flag=False):
        custom_dict = dict()
        for (direction_key, direction_value) in enum.__dict__.items():
            if '__' in direction_key:
                continue

            if inversion_flag:
                custom_dict[direction_value] = direction_key
            else:
                custom_dict[direction_key] = direction_value
        return custom_dict


custom_enum_utils = CustomEnumUtils()
const.DIRECTION_ENUMS = custom_enum_utils.enum(Norm=0, Buy=1, Sell=-1, Creation=2, Redemption=3)

const.ORDER_STATUS_ENUMS = custom_enum_utils.enum(none=-1, New=0, PartialFilled=1, Filled=2, DoneForDay=3,
                                                  Canceled=4, Replace=5, PendingCancel=6, Stopped=7, Rejected=8,
                                                  Suspended=9, PendingNew=10, Calculated=11, Expired=12,
                                                  AcceptedForBidding=13, PendingReplace=14, EndAsSucceed=15,
                                                  Accepted=16, InternalRejected=17)

const.OPERATION_STATUS_ENUMS = custom_enum_utils.enum(none=-1, New=0, PartialFilled=1, Filled=2,
                                                      DoneForDay=3, Canceled=4, Replace=5, PendingCancel=6, Stopped=7,
                                                      Rejected=8, Suspended=9,
                                                      PendingNew=10, Calculated=11, Expired=12, Restated=13,
                                                      PendingReplace=14, Accepted=15,
                                                      SubmitCancel=16, SubmitReplace=17, InternalRejected=18,
                                                      RecoverFILL=-2)

const.ORDER_TYPE_ENUMS = custom_enum_utils.enum(none=0, LimitOrder=1, SingleAlgo=2, BasketAlgo=3, EnhancedLimitOrder=4,
                                                SpecialLimitOrder=5, SelfCross=14)

const.PEG_LEVEL_ENUMS = custom_enum_utils.enum(SameSide=100, OppositeSide=-100)

const.INSTRUMENT_TYPE_ENUMS = custom_enum_utils.enum(ANY=0, Future=1, Spot=2, SPREAD=3, CommonStock=4, PORTFOLIO=5,
                                                     Index=6, MutualFund=7, Warrant=8, Cross=9, Option=10, REIT=11,
                                                     Unit=12, Preference=13, Right=14, MMF=15, StructuredFund=16,
                                                     ReversePurch=18, MainContract=20, ConvertableBond=19)

const.EXCHANGE_TYPE_ENUMS = custom_enum_utils.enum(ANY=0, HK=13, CG=18, CS=19, SHF=20, DCE=21, ZCE=22, CFF=25, INE=35,
                                                   FX=28)

const.MARKETSECTOR_TYPE_ENUMS = custom_enum_utils.enum(Comdty=1, Corp=2, Curncy=3, Equity=4, Govt=5, Index=6, ANY=7)

const.TRADE_TYPE_ENUMS = custom_enum_utils.enum(Normal=0, Short=1, Open=2, Close=3, CloseYesterday=4, RePur=5,
                                                MergeSplit=6, NA=7, Exercise=8, Merge=9, Split=10, Creation=11,
                                                Redeem=12)

const.HEDGEFLAG_TYPE_ENUMS = custom_enum_utils.enum(Speculation=0, Arbitrage=1, Hege=2)

const.IO_TYPE_ENUMS = custom_enum_utils.enum(Inner1=0, Inner2=1, Outer=2)

const.ALGO_TYPE_ENUMS = custom_enum_utils.enum(Peg=0, MarketBasket=3, Pair=8, SigmaVWAP_AI=15, SigmaVWAP=16,
                                               SigmaVWAP_3=17, OIB=18)

const.ALGO_STATUS_ENUMS = custom_enum_utils.enum(Running=0, Paused=1, Finished=2)

const.MARKET_STATUS_ENUMS = custom_enum_utils.enum(NonDefined=0, ACTV=1, SUSP=2, HALT=3, Auct=4, OInd=5, CInd=6,
                                                   UpLimit=7, DownLimit=8, FUSE=9, NotTrade=10)

const.MSG_TYPEID_ENUMS = custom_enum_utils.enum(MarketDataSubscribe=0,
                                                MarketDataResponse=1,
                                                InstrumentInfoRequest=2,
                                                InstrumentInfoResponse=3,
                                                NewOrder=4,
                                                CancelOrder=5,
                                                ReplaceOrder=6,
                                                OrderInfoRequest=7,
                                                OrderInfoResponse=8,
                                                OrderStatusSubscribe=9,
                                                OrderStatusResponse=10,
                                                ServerInfoRequest=11,
                                                ServerInfoResponse=12,
                                                ServerParameterChangeRequest=13,
                                                StrategyInfoRequest=14,
                                                StrategyInfoResponse=15,
                                                StrategyParameterChangeRequest=16,
                                                TradeInfoRequest=17,
                                                TradeInfoResponse=18,
                                                PositionRiskRequest=19,
                                                PositionRiskResponse=20,
                                                TradeServerInfoRequest=21,
                                                TradeServerInfoResponse=22,
                                                SubscribeTradeServerInfoRequest=23,
                                                SubscribeTradeServerInfoResponse=24,
                                                ServerBroadcastInfo=25,
                                                StrategyStateChangeRequest=26,
                                                ClearAbnormalState=27,
                                                StrategyPositionChangeRequest=28,
                                                Max=32,
                                                Login=100)

const.RISK_FREQUENCY_ENUMS = custom_enum_utils.enum(BeforeOpen=0, TradingTimes=1, AfterOpen=2)

const.BASKET_FILE_OPERATION_ENUMS = custom_enum_utils.enum(Add='add', Change='change',
                                                           Cutdown='cutdown', Close='close',
                                                           Close_Bits='close_bits')

const.EOD_POOL = dict()

# 缓存服务器参数数据
const.SERVER_DICT = OrderedDict()
# 缓存服务器分组数据
const.SERVER_GROUP_DICT = OrderedDict()
const.WSDL_DICT = dict()
const.PATH_DICT = dict()
const.EMAIL_DICT = dict()

const.Index_Future_List = ['IC', 'IF', 'IH']
const.TreasuryBond_Future_List = ['T', 'TF']

# const.INTRADAY_ACCOUNT_DICT = {'huabao': ['steady_return', 'absolute_return'],
#                                'citics': ['solid_return', 'deriv_01', 'ch_selection'],
#                                'guosen': ['ch_selection', 'EIF01', 'All_Weather_1', 'All_Weather_2', 'All_Weather_3'],
#                                'zhongtai': ['QS01']}
const.INTRADAY_ACCOUNT_DICT = dict()
