import subprocess
from SimpleXMLRPCServer import SimpleXMLRPCServer
import redis
import AllProtoMsg_pb2

r = redis.Redis(host='172.16.12.118', port=6379, db=3)


def query_market(ticker_list):
    instrument_dict = dict()
    redis_instrument_dict = r.hgetall("Instrument_all")
    for (target_id, instrument_info_str) in redis_instrument_dict.items():
        instrument_msg = AllProtoMsg_pb2.Instrument()
        instrument_msg.ParseFromString(instrument_info_str)
        instrument_dict[str(target_id)] = instrument_msg

    market_dict = dict()
    redis_market_dict = r.hgetall('Market')
    for (dict_key, market_info_str) in redis_market_dict.items():
        market_msg = AllProtoMsg_pb2.MarketDataResponseMsg()
        market_msg.ParseFromString(market_info_str)

        instrument_msg = instrument_dict[dict_key]
        if market_msg.Args.LastPrice:
            market_dict[instrument_msg.ticker] = (market_msg.Args.LastPrice, market_msg.Args.Volume)

    query_ticker_dict = dict()
    for ticker in ticker_list:
        if ticker in market_dict:
            query_ticker_dict[ticker] = market_dict[ticker]
        else:
            query_ticker_dict[ticker] = None
    return query_ticker_dict


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.11.127', 8888))
    s.register_function(query_market)
    s.serve_forever()