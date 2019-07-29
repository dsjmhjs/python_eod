# -*- coding: utf-8 -*-
import httplib
import json
import urllib

try:
    conn = httplib.HTTPConnection("cmds.citicsinfo.com", 800)
    headers = {"Content-Type": "application/json"}
    values = {"USERNAME": "TestForAllWS", "PASSWORD": "000000", "DATA_SOURCE_ID": 10, "API_NAME": "RATES_CURVES",
              "START_PAGE": 1, "PAGE_SIZE": 1000, "START_DATE": "20150716", "END_DATE": "20150717",
              "COLS": (["TERM", "ASK_PRICE"]), "CODES": (["SWAP_FR007"]), "CONDITIONS": "", "API_VERSION": "N"}
    # COLS和CODES赋值时，如果只有一个值的时候，请务必加中括号
    conn.request("POST", "/RunAPI.svc/RunApi", json.JSONEncoder().encode(values), headers)

    res = conn.getresponse()
    data = res.read()
    print data
except Exception, e:
    print e
finally:
    conn.close()
