# -*- coding: utf-8 -*-
import urllib
import urllib2
import pickle
import codecs
from bs4 import BeautifulSoup


def query_grainmarket(type_str, start_date, end_date):
    page_total, a_href_list = __query_by_open(type_str, start_date, end_date)
    for i in range(2, page_total + 1):
        temp_list = __query_by_request(type_str, start_date, end_date, i)
        while len(temp_list) == 0:
            temp_list = __query_by_request(type_str, start_date, end_date, i)
        print i, len(temp_list)
        a_href_list.extend(temp_list)

    # fw = open('url_list.txt', 'wb')
    # pickle.dump(a_href_list, fw, -1)
    # fw.close()
    #
    # fr = open('url_list.txt', 'rb')
    # a_href_list = pickle.load(fr)
    # fr.close()
    __save_web_content(a_href_list)


def __save_web_content(a_href_list):
    print len(a_href_list)
    base_url = 'http://www.grainmarket.com.cn'
    for (title_name, a_href_str) in a_href_list[330:]:
        print title_name
        url = base_url + a_href_str
        page = urllib2.urlopen(url)
        base_soup = BeautifulSoup(page.read(), 'lxml')
        img_flag = False
        for img_item in base_soup.find_all('img'):
            if '/Uploads' in img_item['src']:
                urllib.urlretrieve(base_url + img_item['src'], '%s.jpg' % title_name)
                img_flag = True

        if not img_flag:
            for text_item in base_soup.find_all(class_='info_text_con bor2'):
                text_content = text_item.text
            with codecs.open('%s.txt' % title_name, 'a', encoding='utf-8') as f:
                f.writelines(text_content)


def __query_by_open(type_str, start_date, end_date):
    base_url = 'http://www.grainmarket.com.cn/News/Search/0_0_%s_%s_%s' % (start_date, end_date, type_str)
    page = urllib2.urlopen(base_url)
    base_soup = BeautifulSoup(page.read(), 'lxml')
    total_number = 0
    for div_item in base_soup.find_all(id='ctl00_ContentPlaceHolder1_lblListCount'):
        total_number = int(div_item.text)
    page_size = 20
    page_total = total_number / page_size + 1

    a_href_list = []
    for dt_item in base_soup.find_all('dt'):
        a_href_list.append((dt_item.text, dt_item.contents[0]['href']))
    return page_total, a_href_list


def __query_by_request(type_str, start_date, end_date, page_number):
    base_url = 'http://www.grainmarket.com.cn/News/Search/0_0_%s_%s_%s' % (start_date, end_date, type_str)
    a_href_list = []
    test_data = {'__VIEWSTATE': '/wEPBRI2MzY2ODAzNzYzMDE4MTkxOTdkXApnt87zRjsvX1BBgvadQNMdR8s=',
                 '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$AspNetPager1',
                 '__EVENTARGUMENT': page_number,
                 '__EVENTVALIDATION': '/wEdAAYQmwIs9zv1Ph0QH4NYGIE4gjvT3yZN12ZkMKxvwg8XOZnNWzNY5zSGNpodCe8Yb3ImYVHCvKs6Em7/F+UTwDGBRxa7jbe9GiblAwssofIEqKICDd2ZE72DYhE4IXFXUHTgN4jbWnbMNlB5IGrJvZt4Z397Yw==',
                 'ctl00$ContentPlaceHolder1$txt_KeyWords': type_str,
                 'ctl00$ContentPlaceHolder1$cb_Content': 'on',
                 'ctl00$ContentPlaceHolder1$cb_Title': 'on',
                 'ctl00$ContentPlaceHolder1$txt_StartDate': start_date,
                 'ctl00$ContentPlaceHolder1$txt_EndDate': end_date}
    test_data_urlencode = urllib.urlencode(test_data)
    req = urllib2.Request(url=base_url, data=test_data_urlencode)
    res_data = urllib2.urlopen(req)
    base_soup = BeautifulSoup(res_data.read(), 'lxml')
    for dt_item in base_soup.find_all('dt'):
        a_href_list.append((dt_item.text, dt_item.contents[0]['href']))
    return a_href_list


if __name__ == '__main__':
    type_str = '%E7%8E%89%E7%B1%B3%E4%BA%A4%E6%98%93%E7%BB%93%E6%9E%9C'
    start_date = '2006-01-01'
    end_date = '2018-07-25'
    query_grainmarket(type_str, start_date, end_date)
