import redis
# from eod_aps.tools.date_utils import DateUtils

# date_utils = DateUtils()

def redis_backup_method():
    r = redis.Redis(host='172.16.12.118', port=6379, db=1)
    # info = r.info()
    # for key in info:
    #     print '%s:%s' % (key, info[key])

    dic = {"2017-04-11 09:00:00": "0", "2017-04-11 09:01:00": "100", "2017-04-11 09:02:00": "-200"}
    strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm']
    r.hmset("Long_IndNorm", dic)

    # print r.hkeys("Long_IndNorm")
    # print r.hvals("Long_IndNorm")
    hh_list = r.hkeys("Long_IndNorm")
    hh_list.sort()
    for time_str in hh_list:
        print time_str, r.hget("Long_IndNorm", time_str)

    # print r.hkeys("Long_IndNorm")
    # r.hset("Long_IndNorm", "2017-04-11 09:03:00", "105")
    # print r.hkeys("Long_IndNorm")
    # print r.hvals("Long_IndNorm")
    # print(r.hgetall("Long_IndNorm"))

    # r.lpush("Long_MV10Norm", "2017-04-11 09:00:00,0")
    # r.rpush("Long_MV10Norm", "2017-04-11 09:01:00,100", "2017-04-11 09:02:00,-200", "2017-04-11 09:03:00,105")
    # print r.lrange('Long_MV10Norm', start=0, end=-1)


# def redis_recv():
#     r = redis.Redis(host='172.16.12.118', port=6379, db=1)
#     strategy_name_list = ['Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'ESOP']
#     for strategy_name in strategy_name_list:
#         print strategy_name
#         redis_key = 'market_ret:%s:%s' % (strategy_name, date_utils.get_now().strftime("%Y-%m-%d"))
#         # redis_key = 'market:%s:%s' % (strategy_name, date_utils.get_now().strftime("%Y-%m-%d"))
#         hh_list = r.hkeys(redis_key)
#         hh_list.sort()
#         for time_str in hh_list:
#             print time_str, r.hget(redis_key, time_str)


def get_strategy_list():
    r = redis.Redis(host='172.16.12.118', port=6379, db=2)
    basket_name = 'ESOP'
    key = 'market_ret:%s:ticker_list' % basket_name
    ticker_list = r.lrange(key, 0, -1)
    print ticker_list
    for ticker in ticker_list:
        ticker_key = 'market_ret:%s:%s' % (basket_name, ticker)
        print ticker, r.get(ticker_key).replace('|', ',')


def get_date_info():
    r = redis.Redis(host='172.16.12.118', port=6379, db=2)
    key = 'market:typelist:Event_Real_position'
    basket_list = r.lrange(key, 0, -1)
    print basket_list

    for basket_name in basket_list :
        redis_key = 'market_ret:%s:14:50:00' % basket_name
        print r.hget(redis_key, '2017-07-31 14:50:00')



    # ticker_list = r.lrange('market:typelist:realposition', 0, -1)
    # print ticker_list

if __name__ == '__main__':
    # get_strategy_list()
    get_date_info()


