#!/usr/bin/env python
# send a email with attachment
# $0 <recipent> <subject> <emailbody file> <attchment file>
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
from sys import argv
import ntpath
mail_host = 'smtp.haodou.com'
mail_port = 465
mail_user = 'noreply@haodou.com'
mail_pass = 'josh5vosh8'


def send(to,sub,bodyfile,att):

    msg = MIMEMultipart()
    msg['Subject'] = sub
    msg['From'] = 'noreply@haodou.com'
    msg['To'] = to
    #msg.add_header('Bcc','zhaoweiguo@haodou.com')
    to = [to,'zhaoweiguo@haodou.com']
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(att, "rb").read())
    Encoders.encode_base64(part)
    #body = open(bodyfile,'r').read()
    msg.attach(MIMEText(bodyfile,_subtype='html',_charset='utf-8'))
    filename=ntpath.basename(att)
    part.add_header('Content-Disposition', 'attachment; filename="' + filename + '"' )

    msg.attach(part)

    server = smtplib.SMTP_SSL()
    server.connect(mail_host,mail_port)
    server.login(mail_user,mail_pass)
    server.sendmail(mail_user, to, msg.as_string())


if __name__ == '__main__':

    send(argv[1],argv[2],argv[3],argv[4])