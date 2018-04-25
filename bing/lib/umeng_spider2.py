# !/usr/bin/python
#  -*- coding: utf-8 -*-

import urllib2
import urllib
import re
import cookielib
import datetime
from optparse import OptionParser
import simplejson
import math
import sys
reload(sys)
sys.setdefaultencoding('utf8')

parser = OptionParser()
#parser.add_option("-n", "--number", dest="number", help="number of days", metavar="NUMBER")

(options, args) = parser.parse_args()
#options.number = int(options.number or 1)

response = urllib2.urlopen('https://www.umeng.com/sso/login?service=http://www.umeng.com/apps')
html = response.read()

match = re.search(r"<input type=\"hidden\" id=\"lt\" name=\"lt\" value=\"(.*)\" />", html)

auth_url = 'https://www.umeng.com/sso/login'
home_url = 'http://www.umeng.com/apps';
# 登陆用户名和密码
data={
  "username":"roczhong@gmail.com",
  "password":"hd123456",
  "service":"http:&#x2F;&#x2F;www.umeng.com&#x2F;apps",
  "lt":match.group(1)
}

# urllib进行编码
post_data=urllib.urlencode(data)
# 发送头信息
headers ={
  "Host":"www.umeng.com",
  "Referer": "http://www.umeng.com"
}
# 初始化一个CookieJar来处理Cookie
cookieJar=cookielib.CookieJar()
# 实例化一个全局opener
opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar))
# 获取cookie
req=urllib2.Request(auth_url,post_data,headers)
result = opener.open(req)
# 访问主页 自动带着cookie信息
result = opener.open(home_url)
# 显示结果
apps_html = result.read()

#apps_id = re.findall(r"<li app_id=\"(.*)\">", apps_html)
apps = re.findall(r'''<li app_id=\"(.*)\">
                <img alt=\"(.*)\" title=\".*\" width=\"24\" src=\"/images/icons/.*\.png\"/>
              (.*)
            </li>''', apps_html)

#umeng_app_version
today = datetime.date.today()
fd = open("umeng_app_version_%(date)s.txt" % {'date':today.strftime("%Y%m%d")}, 'w+')
tasks =[]
for (app_id, platform, app_name) in apps:
  task = {'app_name':app_name, 'app_id':app_id, 'platform':platform}
  app_version_url = "http://www.umeng.com/apps/%(app_id)s/reports/load_table_data?day=yesterday&per_page=20&stats=versions_table&game="
  task['app_version_url'] = app_version_url % {'app_id':app_id}
  task['date'] = today - datetime.timedelta(1)
  tasks.append(task)

for t in tasks:
  result = opener.open(t['app_version_url'] + '&page=1' % {'app_id':t['app_id']})
  json = result.read()
  data = simplejson.loads(json)
  page = int(math.ceil(data['total']/20.0))

  while page > 0:
    result = opener.open(t['app_version_url'] + "&page=%(page)s" % {'page':page})
    page -= 1
    json = result.read()
    data = simplejson.loads(json)

    format = "%(statis_date)s\t%(version)s\t%(version_installations)s\t%(install)s\t%(upgrade)s\t%(period_user_count)s\t%(active_user)s\t%(launch)s\t%(app_id)s\t%(data_date)s\n"
    for i in data['stats']:
      line = format % {'app_id':t['app_id'],'version':i['version'], 'version_installations':i['version_installations'], 'install':i['install'], 'upgrade':i['upgrade'], 'period_user_count': i['period_user_count'], 'active_user':i['active_user'], 'launch':i['launch'], 'statis_date': (today - datetime.timedelta(1)).strftime('%Y-%m-%d'), 'data_date': today.strftime('%Y-%m-%d')}
      print line.encode('utf-8')
      fd.write(line)
fd.close()
