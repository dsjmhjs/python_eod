# coding: utf-8
import time
from SimpleXMLRPCServer import SimpleXMLRPCServer
from splinter.browser import Browser


class WebLoginServer(object):
    """
        WebLoginServer
    """
    def __init__(self):
        pass

    def login(self):
        b = Browser(driver_name='chrome')
        b.visit('https://vpn-guest.citicsinfo.com')
        b.fill('svpn_name', 'bj-dongsanhuan-1')
        b.fill('svpn_password', 'EQJtqXC2')
        b.find_by_text(u'登 录').click()
        time.sleep(2)
        # b.find_by_id('sendSms').click()


if __name__ == '__main__':
    s = SimpleXMLRPCServer(('172.16.11.127', 7088))
    web_login_server = WebLoginServer()
    s.register_instance(web_login_server)
    s.serve_forever()

    # scp_server = ServerProxy('http://172.16.11.127:7088')
    # upload_flag = scp_server.upload_file('Z:/dailyjob/ts_order_106.2017-11-28.151118.txt', '/home/trader')
    # print upload_flag
