# -*- coding: utf-8 -*-
# 实现邮件发送
import os
import smtplib
import sys
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email import encoders
from eod_aps.model.eod_const import const

reload(sys)
sys.setdefaultencoding('utf8')


class EmailUtils(object):
    """
        邮件工具类
    """
    host_ip = ''
    sender = ''
    mail_to = None
    # 接收者邮箱
    receiver = None
    smtp_server = ''
    smtp_port = ''
    smtp_username = ''
    smtp_password = ''

    def __init__(self, receiver=''):
        if receiver != '':
            self.receiver = receiver
        self.mail_to = const.EMAIL_DICT['group1'][0]

        self.host_ip = const.EOD_CONFIG_DICT['host_ip']
        self.sender = const.EOD_CONFIG_DICT['smtp_from']
        self.smtp_server = const.EOD_CONFIG_DICT['smtp_server']
        self.smtp_port = const.EOD_CONFIG_DICT['smtp_port']
        self.smtp_username = const.EOD_CONFIG_DICT['smtp_username']
        self.smtp_password = const.EOD_CONFIG_DICT['smtp_password']

    def send_email_path(self, subject, content, file_names, content_type=None):
        msg = MIMEMultipart()
        if content_type == 'html':
            content_msg = MIMEText(content, 'html', 'utf-8')
        else:
            content_msg = MIMEText(content)
        msg.attach(content_msg)
        msg['Subject'] = subject
        msg['From'] = 'EOD[%s]' % self.host_ip
        msg['To'] = self.mail_to
        msg['Cc'] = ';'.join(self.receiver[1:])

        for attachment_file in file_names.split(","):
            contype = 'application/octet-stream'
            maintype, subtype = contype.split('/', 1)
            file_data = open(attachment_file.encode("utf-8"), 'rb')
            file_msg = MIMEBase(maintype, subtype)
            file_msg.set_payload(file_data.read())
            file_data.close()
            encoders.encode_base64(file_msg)
            basename = os.path.basename(attachment_file)
            file_msg.add_header('Content-Disposition', 'attachment', filename=basename.encode("utf-8"))
            msg.attach(file_msg)

        smtp = smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port)
        smtp.login(self.smtp_username, self.smtp_password)
        smtp.sendmail(self.sender, self.receiver, msg.as_string())
        smtp.quit()

    # 初始化类的时候定义receiver，则下面的函数可以作为通用的方法
    def send_email_group_all(self, subject, content, content_type=None):
        if content_type == 'html':
            msg = MIMEText(content, 'html', 'utf-8')
        else:
            msg = MIMEText(content)
        subject = self.format_email_subject(subject, content)
        msg['Subject'] = subject
        msg['From'] = 'EOD[%s]' % self.host_ip
        msg['To'] = self.mail_to
        msg['Cc'] = ';'.join(self.receiver[1:])

        smtp = smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port)
        smtp.login(self.smtp_username, self.smtp_password)
        smtp.sendmail(self.sender, self.receiver, msg.as_string())
        smtp.quit()

    def send_cc_email(self, subject, content):
        mail_to = 'yangzhoujie@derivatives-china.com'
        mail_cc = 'yaojunfei@derivatives-china.com;jinchengxun@derivatives-china.com'
        msg = MIMEText(content)
        msg['Subject'] = subject
        msg['From'] = 'EOD[%s]' % self.host_ip
        msg['To'] = mail_to
        msg['Cc'] = mail_cc
        smtp = smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port)
        smtp.login(self.smtp_username, self.smtp_password)
        smtp.sendmail(self.sender, self.group2, msg.as_string())
        smtp.quit()

    def list_to_html(self, title, info_list):
        html_list = ['<table border="1"><tr>']
        for title_item in title.split(','):
            html_list.append('<th align="center" font-size:12px; bgcolor="#FF7A00"><b>%s</b></th>' % title_item)
        html_list.append('</tr>')

        for info_sub_list in info_list:
            html_list.append('<tr>')
            for index, info_item in enumerate(info_sub_list):
                info_item_str = ''
                if info_item is not None:
                    info_item_str = str(info_item)
                if index == 0:
                    html_list.append(
                        '<td align="center" font-size:12px;bgcolor="#A8A8A8"><b>%s</b></td>' % info_item_str)
                else:
                    if info_item_str.find('(Error)') >= 0:
                        html_list.append('<td align="center" bgcolor="#ee4c50">%s</td>' % \
                                         info_item_str.replace('(Error)', '').replace('(Warning)', ''))
                    elif info_item_str.find('(Warning)') >= 0:
                        html_list.append('<td align="center" bgcolor="#ffd700">%s</td>' % \
                                         info_item_str.replace('(Error)', '').replace('(Warning)', ''))
                    else:
                        html_list.append('<td align="center">%s</td>' % info_item_str)
            html_list.append('</tr>')
        html_list.append('</table>')
        return html_list

    def list_to_html2(self, title, info_list):
        html_list = ['<table border="1"><tr>']
        for title_item in title.split(','):
            html_list.append('<th align="center" font-size:12px; bgcolor="#FF7A00"><b>%s</b></th>' % title_item)
        html_list.append('</tr>')

        for info_sub_list in info_list:
            html_list.append('<tr>')
            for index, info_item in enumerate(info_sub_list):
                info_item = str(info_item)
                if index == 0:
                    html_list.append('<td align="center" font-size:12px; bgcolor="#A8A8A8"><b>%s</b></td>' % info_item)
                else:
                    if self.__is_num(info_item.replace(',', '')):
                        if int(info_item.replace(',', '')) > 0:
                            html_list.append('<td align="right"><font color=red>%s</font></td>' % info_item)
                        elif int(info_item.replace(',', '')) == 0:
                            html_list.append('<td align="right">%s</td>' % info_item)
                        else:
                            html_list.append('<td align="right"><font color=green>%s</font></td>' % info_item)
                    else:
                        html_list.append('<td align="right">%s</td>' % info_item)
            html_list.append('</tr>')
        html_list.append('</table>')
        return html_list

    def send_img_msg(self, subject, table_content, img_path_list):
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = 'EOD[%s]' % self.host_ip
        msg['To'] = self.mail_to
        msg['Cc'] = ';'.join(self.receiver[1:])

        pic_style = 'style="border:1;height:400px;width:80px margin-left:10px"'
        pic_content = []
        for img_conten, img_file_path in img_path_list:
            img_file_name = os.path.basename(img_file_path)
            msg.attach(self.__addimg(img_file_path, img_file_name))
            pic_content.append('<li>%s</li>' % img_conten)
            pic_content.append('<img src="cid:%s" %s>' % (img_file_name, pic_style))
        img_text = MIMEText(table_content + ''.join(pic_content), "html", "utf-8")
        msg.attach(img_text)

        smtp = smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port)
        smtp.login(self.smtp_username, self.smtp_password)
        smtp.sendmail(self.sender, self.receiver, msg.as_string())
        smtp.quit()
        return msg

    # 发送带附件邮件
    def send_attach_email(self, subject, content, attach_list):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = 'EOD[%s]' % self.host_ip
        msg['To'] = self.mail_to
        msg['Cc'] = ';'.join(self.receiver[1:])

        part = MIMEText(content)
        msg.attach(part)

        for attach_path in attach_list:
            part = MIMEApplication(open(attach_path, 'rb').read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attach_path))
            msg.attach(part)

        smtp = smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port)
        smtp.login(self.smtp_username, self.smtp_password)
        smtp.sendmail(self.sender, self.receiver, msg.as_string())
        smtp.quit()

    def __addimg(self, img_path, imgid):
        with open(img_path, 'rb') as fr:
            msg_image = MIMEImage(fr.read())
        msg_image.add_header('Content-ID', imgid)
        return msg_image

    def __is_num(self, x):
        try:
            x = int(x)
            return isinstance(x, int)
        except ValueError:
            return False

    def format_email_subject(self, subject, e_content):
        # if 'error' in subject.lower() or 'warning' in subject.lower() or 'warn' in subject.lower():
        #     return subject
        if '(Error)' in e_content or '#ee4c50' in e_content:
            rst_subject = "[Error] %s" % subject
        elif '(Warning)' in e_content or '#ffd700' in e_content:
            rst_subject = "[Warning] %s" % subject
        else:
            rst_subject = subject
        return rst_subject


if __name__ == '__main__':
    email_utils = EmailUtils(const.EMAIL_DICT['group1'])
    content = ''''''
    email_utils.send_email_group_all('email cc bcc test', content, 'html')
