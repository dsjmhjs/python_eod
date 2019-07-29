# -*- coding: utf-8 -*-
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


browser = webdriver.Ie()
browser.get('https://vpn.nawaa.com/vip')

print browser
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