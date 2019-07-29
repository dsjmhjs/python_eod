#coding=utf-8
from splinter.browser import Browser
import time
url = 'http://127.0.0.1:8080'
b = Browser(driver_name='chrome')
b.visit(url)


b.fill('name', 'admin')
b.fill('password', '123456')

b.find_by_xpath('//*[@id="app"]/form/div[4]/div/button').click()

b.find_by_xpath('/html/body/div/section/aside/ul/li[2]').click()
print(0)
time.sleep(3)
b.find_by_xpath('/html/body/div/section/aside/ul/li[2]/ul/li[3]').click()
time.sleep(3)
print(1)

b.find_by_xpath('/html/body/div/section/section/main/div/button').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[1]/div/div/div[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[3]/div[1]/div[1]/ul/li[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[2]/div/div/div[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[4]/div[1]/div[1]/ul/li[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[3]/div/div/div[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[5]/div[1]/div[1]/ul/li[2]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[4]/div/div[1]/input').fill('xiaoming')
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[5]/div/div[1]/input').fill('123456789')
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[6]/div/div[1]/input').fill('123456789')
time.sleep(1)

b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[7]/div/label[1]/span[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[8]/div/label[1]/span[1]').click()
time.sleep(1)

b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[9]/div/div/div[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[6]/div[1]/div[1]/ul/li[5]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[10]/div/label[1]/span[1]').click()
time.sleep(1)
b.find_by_xpath('/html/body/div[1]/section/section/main/div/div[1]/div/div[2]/div/div/form/div[11]/div/button[1]').click()
time.sleep(1)

b.quit()
