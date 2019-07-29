# WechatPush.py
# encoding: utf-8
import urllib2, json


# "id": 100,
# "name": "公司运维",
# "count": 3
class WechatPush(object):
    appid = 'wx23b6fcc80eb5185b'
    secrect = '873da54aa250e9567cd87b661082bc80'
    open_id_dict = {"o9-ItwWEWKncuQjudyNRbyA4lmDo": "yangzhoujie", "o9-ItwYGHvU4QqCRPgSiccqJHoW8": "yaojunfei",
                    "o9-ItwbAIVMW6fo0GdqKCkInq3EA": "jinchengxun"}
    template_id = 'cciPH331V-ZcYhaP2th9YThJpgSVnG1bajQIR9uvF0w'
    data_dict = dict()

    title = ''
    date_str = ''
    performance = ''
    remark = ''

    def __init__(self):
        pass

    # 获取accessToken
    def getToken(self):
        # 判断缓存
        url = 'https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=' + self.appid + "&secret=" + self.secrect
        f = urllib2.urlopen(url)
        s = f.read()
        # 读取json数据
        j = json.loads(s)
        j.keys()
        token = j['access_token']
        print token
        return token

    # 开始推送
    def do_push_user(self, json_template):
        token = self.getToken()
        requst_url = "https://api.weixin.qq.com/cgi-bin/user/tag/get?access_token=" + token
        content = self.post_data(requst_url, json_template)
        # 读取json数据
        j = json.loads(content)
        return j

    # 开始推送
    def do_push_message(self, touser):
        dict_arr = {'touser': touser, 'template_id': self.template_id, 'url': '', 'topcolor': "#7B68EE", 'data': self.data_dict}
        json_template = json.dumps(dict_arr)
        token = self.getToken()
        requst_url = "https://api.weixin.qq.com/cgi-bin/message/template/send?access_token=" + token
        content = self.post_data(requst_url, json_template)
        # 读取json数据
        j = json.loads(content)
        print j
        j.keys()
        errcode = j['errcode']
        errmsg = j['errmsg']
        return errcode, errmsg

    # 模拟post请求
    def post_data(self, url, para_dct):
        para_data = para_dct
        f = urllib2.urlopen(url, para_data)
        content = f.read()
        return content


    def __build_send_message(self):
        temp1_dict = dict()
        temp1_dict['value'] = self.title
        temp1_dict['color'] = '#7B68EE'
        self.data_dict['first'] = temp1_dict

        temp2_dict = dict()
        temp2_dict['value'] = self.date_str
        temp2_dict['color'] = '#7B68EE'
        self.data_dict['time'] = temp2_dict

        temp3_dict = dict()
        temp3_dict['value'] = self.performance
        temp3_dict['color'] = '#7B68EE'
        self.data_dict['performance'] = temp3_dict

        temp4_dict = dict()
        temp4_dict['value'] = self.remark
        temp4_dict['color'] = '#7B68EE'
        self.data_dict['remark'] = temp4_dict

    def send_message(self, title, date_str, performance, remark):
        self.title = title
        self.date_str = date_str
        self.performance = performance
        self.remark = remark
        self.__build_send_message()

        for to_user in self.open_id_dict.keys():
            self.do_push_message(to_user)

if __name__ == '__main__':
    title = 'send test1'
    date_str = '2016-12-16'
    performance = 'send test2'
    remark = 'send test3'

    wechat_push = WechatPush()
    wechat_push.send_message(title, date_str, performance, remark)

