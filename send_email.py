#!/usr/bin/env python3
# send a email with attachment
# $0 <recipent> <subject> <emailbody file> <attchment file>
import smtplib
from email.message import EmailMessage
import mimetypes
import os
from argparse import ArgumentParser

mail_host = 'localhost'
mail_port = 25
mail_user = 'cswgzhao'
mail_pass =  None

def send(args):
    msg = EmailMessage()
    msg['Subject'] = args.subject
    msg['From'] = 'noreply@localhost'
    msg['To'] = args.recipents
    msg.set_content(open(args.content,'r').read(), subtype='html')
    #body = open(bodyfile,'r').read()

    if args.atts:
        for att in args.atts:
            filepath, filename = os.path.split(att)
            ctype, encoding = mimetypes.guess_type(att)
            if ctype is None or encoding is not None:
                ctype = '/application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            with open(att,'rb') as fp:
                msg.add_attachment(fp.read(), maintype=maintype,
                            subtype=subtype, filename=filename)
            msg.add_header('Content-Disposition', 'attchement', 
                            filename=('utf-8', '', filename) )
    if mail_port == 465:
        server = smtplib.SMTP_SSL(mail_host, mail_port)
    else:
        server = smtplib.SMTP(mail_host, mail_port)
    if mail_user and mail_pass:
        server.login(mail_user,mail_pass)
    server.sendmail(mail_user, args.recipents, msg.as_string())

if __name__ == '__main__':
    parser = ArgumentParser(description="send a mail with attachment")
    parser.add_argument("-r", "--recipents", dest='recipents', nargs='+', 
                        help="mail recipents", required=True)
    parser.add_argument('-s', '--subject', dest='subject', 
                        help='mail subject', required=True)
    parser.add_argument('-c', '--content', dest='content', 
                        help='mail body file', required=True)
    parser.add_argument('-a', '--attachments', dest='atts', nargs='+', help='attchements')
    
    args = parser.parse_args()
    print(args)
    send(args)