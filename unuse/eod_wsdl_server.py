# coding: utf-8
__author__ = 'orangleliu'
__version__ = '0.1'

''''' 
filename: soaplib_test.py 
createdate: 2014-05-10 
comment: webservice 简单学习 

这是官网的一个demo  调试看看 
参考链接： 
http://soaplib.github.io/soaplib/2_0/pages/helloworld.html#declaring-a-soaplib-service 
http://www.cnblogs.com/grok/archive/2012/04/29/2476177.html 

直接执行pyhton文件就可以把webservice启动了 
服务启动之后可以在浏览器： http://localhost:7789/?wsdl 
得到一个xml文件，具体怎么解读还需要查看资料 

需要研究下怎么手动写一个http客户端来请求webservice 
'''
import soaplib
from soaplib.core.service import rpc, DefinitionBase, soap
from soaplib.core.model.primitive import String, Integer
from soaplib.core.server import wsgi
from soaplib.core.model.clazz import Array
from eod_aps.wsdl.strategy_statistics_server import query_strategy_info, query_strategy_money_long


class EodWorldService(DefinitionBase):
    @soap(String, Integer, _returns=Array(String))
    def say_hello(self, name, times):
        results = []
        for i in range(0, times):
            results.append('Hello, %s' % name)
        return results

    # @soap(String, _returns=String)
    # def algo_file_build_server(self, pf_account_plan_str):
    #     algo_file_build_server(pf_account_plan_str)
    #     return 'algo file build success'

    @soap(String, _returns=String)
    def query_strategy_info(self, strategy_name):
        return query_strategy_info(strategy_name)

    @soap(String, _returns=Array(String))
    def query_strategy_money_long(self, strategy_name):
        return query_strategy_money_long(strategy_name)

def start_wsdl():
    try:
        from wsgiref.simple_server import make_server
        soap_application = soaplib.core.Application([EodWorldService], 'tns')
        wsgi_application = wsgi.Application(soap_application)
        server = make_server('0.0.0.0', 7789, wsgi_application)
        print 'soap server starting......'
        server.serve_forever()
    except ImportError:
        print "Error: example server code requires Python >= 2.5"

if __name__ == '__main__':
    start_wsdl()
    # for strategy_name in ('Long_IndNorm', 'Long_MV10Norm', 'Long_Norm', 'Long_MV5Norm', 'ZZ500_Norm', 'CSI300_MV10Norm'):
    # for strategy_name in ('Long_MV10Norm', 'Long_MV5Norm'):
    #     print '\n'.join(query_strategy_money_long(strategy_name))