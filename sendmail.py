#!/usr/bin/python
# -*- coding: utf-8 -*-
import smtplib
import base64
import sys
import os
import re
import random
#from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)
from email.MIMEText import MIMEText


def qqgen(length=7,count=10000):
    """
    qq email  generate,length means the qq id length,count means the number
    """
    qq=[]
    i=0
    cnt = pow(10,length)
    if cnt < count:
        count = cnt
    
    while i < count:
        tmp=str(random.randint(1,9))
        tmp = tmp + ''.join(map(str,(random.sample(range(0,10),length - 1)))) + '@qq.com'
        qq.append(tmp)
        i = i + 1
    return qq

class SendMail:
    """
    simple email sender 
    parameters:
    emails: list of email whill be sent ,such as ['11@qq.com','u@gmail.com']
    stmp_server: stmp server
    sender: sender information
    username: sender account
    password: sender password
    text_type: the type of email body,typical values are plain,html ,xml
    template: the body fo email
    subject: the title of email
    """
    def __init__(self,emails,smtp_server='smtp.qq.com',sender='淘宝皇冠卖家<wgzhao@kingbase.com.cn>',
                 username='10743425@qq.com',password='password',text_type='html',template='body.html',
                 subject='精挑细选，秋冬女装当季热卖，榜单推荐！总有一款合适您！'):

        self.emails = emails
        self.smtp_server = stmp_server
        self.sender = sender
        self.username = username
        self.password = password
        self.text_type = text_type 
        self.template = template
        self.subject = subject
        
    def send(self):
        content=open(self.template).read()
        try:
            msg = MIMEText(content, self.text_type)
            msg['Subject']=       self.subject
            msg['From']   = self.sender # some SMTP servers will do this automatically, not all

            conn = SMTP(self.stmp_server)
            conn.set_debuglevel(False)
            conn.login(self.username, self.password)
            try:
                while email in self.emails:
                    conn.sendmail(self.sender, email, msg.as_string())
            except Exception,err:
                print "send to %s error:%s" % (email,str(err))
                continue
            finally:
                conn.close()

        except Exception, exc:
            print  "mail failed: %s" % str(exc)  # give a error message
            return False
        return True

if __name__ == "__main__":

    emails = qqgen(7,10000)
    st = SendMail(emails,subject='2012年终清仓 天猫商城羽绒服 199元包邮特荐！',
        username='天猫羽绒服专场<xtuer@vip.qq.com>',password='wore431wham447',template='')
    st.send()
