# coding: utf-8
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from xmlrpclib import ServerProxy


class WebLoginServer(object):
    """
        自动登录网站工具
    """
    def __init__(self):
        pass

    def login(self):
        browser = webdriver.Ie()
        browser.get('https://vpn.nawaa.com/vip')
        # elem = browser.find_element_by_name("username")
        # elem.send_keys("vipsh166")
        #
        # elem = browser.find_element_by_name("password")
        # elem.send_keys("Vipsh166@021")

        elem = browser.find_element_by_name("btnSubmit")
        elem.click()

        time.sleep(2)
        try:
            elem = browser.find_element_by_name("btnContinue")
            elem.click()
            time.sleep(2)
        except NoSuchElementException:
            pass

        elem = browser.find_element_by_name("btnWSAMStart")
        elem.click()
        time.sleep(5)


if __name__ == '__main__':
    # s = SimpleXMLRPCServer(('172.16.10.182', 7088))
    # web_login_server = WebLoginServer()
    # s.register_instance(web_login_server)
    # s.serve_forever()

    s = ServerProxy('http://172.16.10.182:7088')
    s.login()

