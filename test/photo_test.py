# -*- coding: utf-8 -*-
# 上面都是导包，只需要下面这一行就能实现图片文字识别
from PIL import Image
import pytesseract


# text = pytesseract.image_to_string(Image.open(u'4月12日国家临储玉米交易结果2018-04-12.jpg'),lang='chi_sim')
# print(text)

# def triangles():
#     l, index = [1], 0
#     while index < 10:
#         yield l
#         l = [1] + [l[i] + l[i + 1] for i in range(len(l) - 1)] + [1]
#         index += 1
#
# n = 0
# results = []
# for t in triangles():
#     results.append(t)
#     n = n + 1
#     if n == 10:
#         break
# print results


# def prod(L):
#     return reduce(lambda x, y: x * y, L)
#
# print('3 * 5 * 7 * 9 =', prod([3, 5, 7, 9]))

def __status_round(test_list):
    for test_num in test_list:
        if test_num % 3 == 0:
            yield True
        else:
            yield False


def query_status():
    test_list = [0,1,2,3,4,5,6,7,8]
    status_dict = {}
    i = 0
    for test_status in __status_round(test_list):
        status_dict[test_list[i]] = test_status
        i += 1
    print status_dict

query_status()




