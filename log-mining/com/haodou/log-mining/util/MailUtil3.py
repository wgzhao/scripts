# -*- coding: UTF-8 -*- 
#coding = utf-8

from email.Header import Header
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
import smtplib


class MailSender:
        
        def __init__(self, file_path="", filename=""):
                #self.mail_host="192.168.1.90"
                self.mail_host="10.1.1.8"
                self.mail_user="noreply@haodou.com"
                self.mail_pass="josh5vosh8"
                self.mail_postfix="haodou.com"
                self.mail_port = 465
                self.attachment_path = file_path
                self.filename = filename
                
                
        def sendMail(self, to_list, sub, content):
        
                me = "Admin <admin@" + self.mail_postfix + ">"
                msg = MIMEMultipart()
                        
                msg["Accept-Charset"]="ISO-8859-1,utf-8"
                msg['Subject'] = sub
                msg['From'] = me
                msg['To'] = to_list
                txt=MIMEText(content, 'html')
                txt.set_charset("utf-8")
                msg.attach(txt)
                if self.attachment_path !="":
                        msg.attach(self.attach())
                try:
                        s = smtplib.SMTP_SSL()
                        s.connect(self.mail_host, self.mail_port)
                        s.login(self.mail_user, self.mail_pass)
                        s.sendmail(me, to_list, msg.as_string())
                        s.close()
                        print '发送成功！'
                        return True
                except Exception, e:
                        print '发送失败！'
                        print str(e)
                        
                return False
                
        def attach(self):
                
                attachment=MIMEText(open(self.attachment_path,"rb").read(),"base64","utf-8")
                
                attachment["Content-Type"]="application/octet-stream"
                attachment["Content-Disposition"]='attachment;filename="%s"'%self.filename
                
                return attachment
                
