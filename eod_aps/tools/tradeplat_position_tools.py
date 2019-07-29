# -*- coding: utf-8 -*-
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.tradeplat_message_tools import *

custom_enum_utils = CustomEnumUtils()
exchange_type_dict = custom_enum_utils.enum_to_dict(const.EXCHANGE_TYPE_ENUMS)


class InstrumentView(object):
    """
        Instrument数据展示类
    """
    def __init__(self, instrument_msg, market_msg):
        self.Index = instrument_msg.index
        self.ID = instrument_msg.id

        self.Ticker = instrument_msg.ticker
        # self.ExchangeID = exchange_type_dict[instrument_msg.ExchangeIDWire]
        self.ExchangeID = instrument_msg.ExchangeIDWire
        self.UniqueSymbol = '%s %s' % (self.Ticker, self.ExchangeID)
        self.MarketSectorID = instrument_msg.MarketSectorIDWire
        self.TypeID = instrument_msg.TypeIDWire
        self.Type2ID = instrument_msg.Type2IDWire
        self.Name = instrument_msg.name
        self.BidMargin = instrument_msg.BidMargin
        self.AskMargin = instrument_msg.AskMargin
        self.ValPT = instrument_msg.ValPT
        self.BuyCommission = instrument_msg.BuyCommission
        self.SellCommission = instrument_msg.SellCommission
        self.ShortSellCommission = instrument_msg.ShortSellCommission
        self.Slippage = instrument_msg.Slippage
        self.CostPerContract = instrument_msg.CostPerContract
        self.Strike = instrument_msg.Strike
        self.CallPut = instrument_msg.CallPut
        self.ExpireDate = instrument_msg.ExpireDate
        # instrument_msg.tickSizeWired
        # self.Session = instrument_msg.sessionWired
        self.Crncy = instrument_msg.crncyWired
        self.BaseCrncy = instrument_msg.basecrncyWired
        self.Close = instrument_msg.closeWired
        self.PrevClose = instrument_msg.prevCloseWired
        self.RoundLotSize = instrument_msg.roundLotWired
        self.MarketStatus = instrument_msg.marketStatusWired
        self.CrossMarket = instrument_msg.CrossMarket
        if instrument_msg.trackunderlyingTickersWire is None or instrument_msg.trackunderlyingTickersWire == '':
            self.TrackUnderlyingTickers = []
        else:
            self.TrackUnderlyingTickers = instrument_msg.trackunderlyingTickersWire.split(';')

        if instrument_msg.underlyingTickersWire is None or instrument_msg.underlyingTickersWire == '':
            self.UnderlyingTickers = []
        else:
            self.UnderlyingTickers = instrument_msg.underlyingTickersWire.split(';')

        self.Underlyings = []
        self.UndlChangeRatio = 0.0
        self.UndlChangeChecked = False
        self.CrashRiskChecked = False
        self.IsOption = self.TypeID == 10
        if self.TypeID == 9 or self.TypeID == 2:
            self.IsCrncy = True
        else:
            self.IsCrncy = False

        if self.TypeID == 1 or self.TypeID == 4 or self.TypeID == 7 or self.TypeID == 15 or self.TypeID == 16:
            self.IsDeltaOne = True
        else:
            self.IsDeltaOne = False

        # self.TheoreticalPrice
        self.NominalPrice = market_msg.Args.NominalPrice
        if self.IsDeltaOne:
            self.Delta = 1
        else:
            self.Delta = market_msg.Args.Delta
        self.Gamma = market_msg.Args.Gamma
        self.Vega = market_msg.Args.Vega
        self.Theta = market_msg.Args.Theta


class RiskView(object):
    """
        Risk数据展示类
    """
    trading_pl = 0.0
    fee = 0.0
    position_pl = 0.0
    total_pl = 0.0
    total_stocks_value = 0.0
    total_future_value = 0.0
    delta = 0.0
    gamma = 0.0
    vega = 0.0
    theta = 0.0

    def  __init__(self, instrument_view, position_msg, account_name):
        self.AccountName = account_name
        self.FundName = account_name.split('-')[2]
        self.Symbol = instrument_view.UniqueSymbol
        # self.LastPrice = instrument.LastPrice
        self.NominalPrice = instrument_view.NominalPrice
        self.UndlChangeChecked = instrument_view.UndlChangeChecked
        self.IsOption = instrument_view.IsOption

        self.net = position_msg.Long - position_msg.Short
        self.long = position_msg.Long
        self.short = position_msg.Short
        self.long_available = position_msg.LongAvailable
        # self.long_cost = Long == 0?0: extraIP.LongCost / core_.ValPT / Math.Abs( Long )
        self.short_available = position_msg.ShortAvailable
        # self.short_cost = Short == 0?0: extraIP.ShortCost / core_.ValPT / Math.Abs( Short )
        self.daylong = position_msg.DayLong
        self.dayshort = position_msg.DayShort
        self.ydlong_remain = position_msg.YdLongRemain
        self.ydshort_remain = position_msg.YdShortRemain
        self.Ticker = instrument_view.Ticker

        self.fee = position_msg.DayTradeFee
        self.trading_pl = self.__trading_pl(instrument_view, position_msg)
        self.position_pl = self.__position_pl(instrument_view, position_msg)
        self.total_pl = self.trading_pl + self.position_pl
        self.total_bought_value = self.__total_bought_value(instrument_view, position_msg)
        self.total_sold_value = self.__total_sold_value(instrument_view, position_msg)
        self.delta = self.__delta(instrument_view, position_msg)
        self.gamma = self.__gamma(instrument_view, position_msg)
        self.vega = self.__vega(instrument_view, position_msg)
        self.theta = self.__theta(instrument_view, position_msg)

        self.total_stocks_value = self.__total_stocks_value(instrument_view, position_msg)
        self.total_future_value = self.__total_future_value(instrument_view, position_msg)

    def __trading_pl(self, instrument_view, position_msg):
        if instrument_view.Ticker.startswith('204'):
            return 0

        if instrument_view.UndlChangeChecked:
            if instrument_view.IsOption:
                return (position_msg.DayLong - position_msg.DayShort) * (
                        instrument_view.TheoreticalPrice - instrument_view.NominalPrice) * instrument_view.ValPT - position_msg.DayTradeFee
            else:
                return (
                               position_msg.DayLong - position_msg.DayShort) * instrument_view.NominalPrice * instrument_view.UndlChangeRatio * instrument_view.ValPT - position_msg.DayTradeFee
        elif instrument_view.CrashRiskChecked:
            if instrument_view.IsOption:
                return (position_msg.DayLong - position_msg.DayShort) * (
                        instrument_view.TheoreticalPrice - instrument_view.NominalPrice) * instrument_view.ValPT - position_msg.DayTradeFee
            else:
                return (
                               position_msg.DayLong - position_msg.DayShort) * instrument_view.NominalPrice * instrument_view.CrashPara.undlChgRatio * instrument_view.ValPT - position_msg.DayTradeFee

        if instrument_view.IsCrncy:
            return position_msg.DayLong - position_msg.DayShort - position_msg.DayTradeFee
        else:
            notional_price = instrument_view.NominalPrice * instrument_view.ValPT
            return position_msg.DayLong * notional_price - position_msg.DayLongCost + position_msg.DayShortCost + position_msg.DayShort * (
                -notional_price) - position_msg.DayTradeCommission - position_msg.DayTradeFee

    def __position_pl(self, instrument_view, position_msg):
        if instrument_view.Ticker.startswith('204') or instrument_view.TypeID == 15:
            return 0
        if instrument_view.UndlChangeChecked:
            if instrument_view.IsOption:
                return position_msg.PrevNet * (
                        instrument_view.TheoreticalPrice - instrument_view.NominalPrice) * instrument_view.ValPT
            else:
                return position_msg.PrevNet * instrument_view.NominalPrice * instrument_view.UndlChangeRatio * instrument_view.ValPT
        elif instrument_view.CrashRiskChecked:
            if instrument_view.IsOption:
                return position_msg.PrevNet * (
                        instrument_view.TheoreticalPrice - instrument_view.NominalPrice) * instrument_view.ValPT
            else:
                return position_msg.PrevNet * instrument_view.NominalPrice * instrument_view.CrashPara.undlChgRatio * instrument_view.ValPT
        return position_msg.PrevNet * (instrument_view.NominalPrice - instrument_view.PrevClose) * instrument_view.ValPT

    def __total_bought_value(self, instrument_view, position_msg):
        return position_msg.DayLongCost

    def __total_sold_value(self, instrument_view, position_msg):
        return position_msg.DayShortCost

    def __delta(self, instrument_view, position_msg):
        if instrument_view.Ticker.startswith("T") and instrument_view.ExchangeID == 25:
            return 0
        if instrument_view.Ticker.startswith('204') or instrument_view.TypeID == 15:
            return 0

        Net = position_msg.Long - position_msg.Short
        if instrument_view.UndlChangeChecked:
            if instrument_view.IsOption:
                return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.Underlyings.First().NominalPrice * (
                        1 + instrument_view.UndlChangeRatio)
            else:
                return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.NominalPrice * (
                        1 + instrument_view.UndlChangeRatio)
        elif instrument_view.CrashRiskChecked:
            if instrument_view.IsOption:
                return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.Underlyings.First().NominalPrice * (
                        1 + instrument_view.CrashPara.undlChgRatio)
            else:
                return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.NominalPrice * (
                        1 + instrument_view.CrashPara.undlChgRatio)

        if instrument_view.Underlyings is None or len(instrument_view.Underlyings) == 0 or instrument_view.IsDeltaOne:
            return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.NominalPrice
        else:
            return Net * instrument_view.Delta * instrument_view.ValPT * instrument_view.Underlyings[0].NominalPrice

    def __gamma(self, instrument_view, position_msg):
        Net = position_msg.Long - position_msg.Short
        if instrument_view.UndlChangeChecked:
            if instrument_view.IsOption:
                return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.Underlyings.First().NominalPrice * (
                        1 + instrument_view.UndlChangeRatio)
            else:
                return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.NominalPrice * (
                        1 + instrument_view.UndlChangeRatio)
        elif instrument_view.CrashRiskChecked:
            if instrument_view.IsOption:
                return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.Underlyings[
                    0].NominalPrice * (1 + instrument_view.CrashPara.undlChgRatio)
            else:
                return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.NominalPrice * (
                        1 + instrument_view.CrashPara.undlChgRatio)

        if instrument_view.Underlyings is None or len(instrument_view.Underlyings) == 0 or instrument_view.IsDeltaOne:
            return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.NominalPrice
        else:
            return Net * instrument_view.Gamma * instrument_view.ValPT * instrument_view.Underlyings[0].NominalPrice

    def __vega(self, instrument_view, position_msg):
        return (position_msg.Long - position_msg.Short) * instrument_view.Vega * instrument_view.ValPT

    def __theta(self, instrument_view, position_msg):
        return (position_msg.Long - position_msg.Short) * instrument_view.Theta * instrument_view.ValPT

    def __total_stocks_value(self, instrument_view, position_msg):
        if instrument_view.TypeID == 4 or instrument_view.TypeID == 7 or instrument_view.TypeID == 16:
            return self.delta
        else:
            return 0

    def __total_future_value(self, instrument_view, position_msg):
        if instrument_view.TypeID == 1:
            return self.delta
        else:
            return 0

    def print_info(self):
        print 'AccountName:%s,Ticker:%s,Trading PL:%s,Position PL:%s,Total PL:%s,Total Stocks Value:%s,\
        Total Future Value:%s,Delta:%s,Gamma:%s' % (self.AccountName, self.Ticker, self.trading_pl,
                                                    self.position_pl, self.total_pl, self.total_stocks_value,
                                                    self.total_future_value, self.delta, self.gamma)


if __name__ == '__main__':
    print '1111'
