#/usr/bin/env python3
# check all database backup is valid or not
# location: 10.90.50.74:/data/gitlab-runner/bin:gitlab-runner
from datetime import datetime
from datetime import timedelta
from operator import sub
import pandas as pd
import subprocess
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import os
import requests

remote_user="root"

check_url = "https://example.com/db_backup_check.json"

ts = datetime.now().strftime("%Y年%m月%d日")

table_template = '''
<html>
  <style type="text/css">
    .mystyle {
        font-size: 11pt; 
        font-family: Arial;
        border-collapse: collapse; 
        border: 1px solid silver;

    }
    .mystyle td, th {
        padding: 5px;
    }

    .mystyle tr:nth-child(even) {
        background: #E0E0E0;
    }

    .mystyle tr:hover {
        background: silver;
        cursor: pointer;
    }
  </style>
  <body>
  <div>
  <h3><center>金融科技部数据库备份自动巡检</center></h3>
  <h4><center>''' + ts + '''</center></h4>'''

send_to = [""]

def send_mail(send_from, send_to, subject, text, files=None, server=None):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text, 'html'))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server, 25)
    smtp.login('email username', 'password')
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def send_file_to_wechat(fpath: str = None):
    """
    send content to work wechat group
    ref: https://developer.work.weixin.qq.com/document/path/91770
    """
    wehcat_sms_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<key>"
    wechat_file_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=<key>&type=file"
    if not os.path.exists(fpath):
        return False
    session = requests.session()
    session.proxies = {
        "https": "http://127.0.0.1:80"
    }
    # upload file and get media_id
    payload = {
        "name": "media",
        "filename": os.path.basename(fpath),
        "filelength": os.stat(fpath).st_size,
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    
    resp = session.post(wechat_file_url, data=payload, files={"media": open(fpath,'rb')})
    if resp.status_code == 200:
        media_id = resp.json()['media_id']
        # send it to wechat bot
        session.post(wehcat_sms_url, json={"msgtype": "file", "file": {"media_id": media_id}})
        session.close()
        return True
    else:
        session.close()
        return False

def send_msg_to_wechat(content: str = None):
    wehcat_sms_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<key>"
    
def sms_alert_msg(item):
    pass

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

if __name__ == '__main__':
    today = datetime.now()
    result = []
    # get check list
    print("[{}]: check database backup status ....".format(today))
    need_check_db = requests.get(check_url, headers={"content-type": "application/json"}).json()
    for item in need_check_db:
        reldate = today - timedelta(days=item.get('offset', 0))
        relpath = reldate.strftime(item.get('path'))
        cur_db = {'用途': item.get('desc'), 'IP地址': item.get('host'), 
            '数据库类型': item.get('type'), '备份文件名': relpath,
            }
        remote_host = "{}@{}".format(remote_user, item.get('host'))
        print("check {}:{} ....".format(remote_host, relpath))
        cmd = "ssh -o ConnectTimeout=5 {} ls -s {}".format(remote_host, relpath)
        sts, output = subprocess.getstatusoutput(cmd)
        if (sts > 0 ):
            print("ssh error: {}".format(output))
            sms_alert_msg(item)
            cur_db['备份大小'] =  '缺失'
        else:
            file_size = int(output.split(" ")[0].strip()) * 1024 #Bytes
            cur_db['备份大小'] = sizeof_fmt(file_size)
        result.append(cur_db)

    df = pd.DataFrame(result)
    dstr=today.strftime('%Y-%m-%d')
    fpath='/tmp/数据库备份巡检结果-{}.xlsx'.format(dstr)
    df.to_excel(fpath, index=False)
    body = table_template + df.to_html(index=False, classes="mystyle") + "</div></body></html"
    send_mail("sender", send_to, "Title({})".format(dstr), body, [fpath,])
    # remove excel file
    send_file_to_wechat(fpath)
    os.remove(fpath)
    print("[{}]: check database backup status done".format(datetime.now()))
