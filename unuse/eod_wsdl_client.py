from suds.client import Client

if __name__ == '__main__':
    wsdl_client = Client('http://172.16.11.106:7789/?wsdl', cache=None)
    print wsdl_client.service.stop_aggregator()

    # result_list = hello_client.service.query_strategy_money_long('Long_MV10Norm')
    # for result_item in result_list:
    #     print result_item