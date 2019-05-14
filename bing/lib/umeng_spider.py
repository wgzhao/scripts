# !/usr/bin/python
#  -*- coding: utf-8 -*-

import urllib2
import urllib
import re
#from bs4 import BeautifulSoup
import cookielib
import datetime
from optparse import OptionParser
import simplejson
import math

parser = OptionParser()
parser.add_option("-n", "--number", dest="number", help="number of days", metavar="NUMBER")

(options, args) = parser.parse_args()
options.number = int(options.number or 1)

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

today = datetime.date.today()
queries = []
start_date = today - datetime.timedelta(options.number)
end_date = today
while(start_date.year < end_date.year):
  queries.append({'start_date':start_date, 'end_date':datetime.date(day=31, month=12, year=start_date.year)})
  start_date = datetime.date(day=1, month=1, year=start_date.year+1)
queries.append({'start_date':start_date, 'end_date':end_date})

tasks = []
for (app_id, platform, app_name) in apps:
  for query in queries:
    task = {'app_name':app_name, 'app_id':app_id, 'platform':platform}
    total_detail_url = "http://www.umeng.com/apps/%(app_id)s/reports/load_table_data?start_date=%(start_date)s&end_date=%(end_date)s&stats=index_details" % {'app_id':app_id, 'start_date': query['start_date'].strftime('%Y-%m-%d'), 'end_date':query['end_date'].strftime('%Y-%m-%d')}
    task['total_detail_url'] = total_detail_url
    #task['pages'] = int((query['end_date'] - query['start_date']).days / 20) + 1
    ###task['year'] = query['start_date'].year
    task['start_date'] = query['start_date']
    task['end_date'] = query['end_date']
    tasks.append(task)

fd = open("umeng_index_details_%(date)s.txt" % {'date':today.strftime("%Y%m%d")}, 'w+')
format = "%(statis_date)s\t%(install_data)s\t%(accumulations)s\t%(active_data)s\t%(install_rate)s\t%(launch_data)s\t%(duration_data)s\t%(upload_data)s\t%(download_data)s\t%(app_code)s\t%(data_date)s\n"
for t in tasks:
  result = opener.open(t['total_detail_url'])
  json = result.read()
  data = simplejson.loads(json)
 
  print t['app_id'] + '@page_1' + '@' + t['start_date'].strftime('%Y-%m-%d') + '~' + t['end_date'].strftime('%Y-%m-%d')
  for i in data['stats']:
    ###line = format % {'statis_date':str(t['year'])+'-'+i['date'], 'install_data':i['install_data'], 'accumulations':i['accumulations'], 'active_data':i['active_data'], 'install_rate':i['install_rate'], 'launch_data':i['launch_data'], 'duration_data':i['duration_data'], 'upload_data':i['upload_data'], 'download_data':i['download_data'], 'app_code':t['app_id'], 'data_date': today.strftime('%Y-%m-%d')}
    line = format % {'statis_date':i['date'], 'install_data':i['install_data'], 'accumulations':i['accumulations'], 'active_data':i['active_data'], 'install_rate':i['install_rate'], 'launch_data':i['launch_data'], 'duration_data':i['duration_data'], 'upload_data':i['upload_data'], 'download_data':i['download_data'], 'app_code':t['app_id'], 'data_date': today.strftime('%Y-%m-%d')}
    #print line
    fd.write(line)
  
  page = int(math.ceil(data['total']/20.0))
  while page > 1:
    result = opener.open(t['total_detail_url'] + "&page=%(page)s" % {'page':page})
    print t['app_id'] + '@page_' + str(page) + '@' + t['start_date'].strftime('%Y-%m-%d') + '~' + t['end_date'].strftime('%Y-%m-%d')
    page -= 1
    json = result.read()
    data = simplejson.loads(json)
 
    for i in data['stats']:
      ###line = format % {'statis_date':str(t['year'])+'-'+i['date'], 'install_data':i['install_data'], 'accumulations':i['accumulations'], 'active_data':i['active_data'], 'install_rate':i['install_rate'], 'launch_data':i['launch_data'], 'duration_data':i['duration_data'], 'upload_data':i['upload_data'], 'download_data':i['download_data'], 'app_code':t['app_id'], 'data_date': today.strftime('%Y-%m-%d')}
      line = format % {'statis_date':i['date'], 'install_data':i['install_data'], 'accumulations':i['accumulations'], 'active_data':i['active_data'], 'install_rate':i['install_rate'], 'launch_data':i['launch_data'], 'duration_data':i['duration_data'], 'upload_data':i['upload_data'], 'download_data':i['download_data'], 'app_code':t['app_id'], 'data_date': today.strftime('%Y-%m-%d')}
      fd.write(line)
      #print line
  



