# -*- coding: UTF-8 -*- 
#coding = utf-8

import sys

from email.Header import Header
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
import smtplib

def getFileName(path):
	p=path.rfind("/")
	return path[p+1:]

class MailSender:
		
		def __init__(self):
				#self.mail_host="192.168.1.90"
				self.mail_host="10.1.1.8"
				self.mail_user="noreply@haodou.com"
				self.mail_pass="josh5vosh8"
				self.mail_postfix="haodou.com"
				self.mail_port = 465 
				
				
		def sendMail(self, to_list, sub, content,file_paths=[]):
		
				me = "zhangzhonghui <admin@" + self.mail_postfix + ">"
				msg = MIMEMultipart()
						
				msg["Accept-Charset"]="ISO-8859-1,utf-8"
				msg['Subject'] = sub
				msg['From'] = me
				msg['To'] = to_list
				txt=MIMEText(content, 'html')
				txt.set_charset("utf-8")
				msg.attach(txt)
				self.attach_files(msg,file_paths)
				try:
						s = smtplib.SMTP_SSL()
						s.connect(self.mail_host)
						s.login(self.mail_user,self.mail_pass)
						s.sendmail(me, to_list, msg.as_string())
						s.close()
						print '发送成功！'
						return True
				except Exception, e:
						print '发送失败！'
						print str(e)
						
				return False
				
		def attach_files(self,msg,file_paths):
				filenames={}
				for attachment_path in file_paths:
						filename=getFileName(attachment_path)
						while filename in filenames:
								filename+="0"
						msg.attach(self.attach(attachment_path,filename))

		def attach(self,attachment_path,filename=""):
				if filename == "":
					filename=getFileName(attachment_path)
				attachment=MIMEText(open(attachment_path,"rb").read(),"base64","utf-8")
				
				attachment["Content-Type"]="application/octet-stream"
				attachment["Content-Disposition"]='attachment;filename="%s"'%filename
				
				return attachment
				
if __name__=="__main__":
	#print getFileName("../../util/eggs.csv")
	sender=MailSender()
	#sender.sendMail("zhangzhonghui@haodou.com","test","haha!!",["../log/userGroupPush/eggs.csv","unicode.py"])
	to_list=sys.argv[1]
	title=sys.argv[2]
	content=sys.argv[3]
	attachment=sys.argv[4:]
	sender.sendMail(to_list,title,content,attachment)


