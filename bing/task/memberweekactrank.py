#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import logging
from optparse import OptionParser
import string, codecs
import subprocess
import MySQLdb
import json, phpserialize
import httplib, urllib, socket
from urlparse import urlparse
import re

def mklogfile(s):
  if not os.path.exists(s):
    f=open(s,'w')
    f.write('.log\n')
    f.close()
    os.chmod(s, 0666)

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),'%Y%m%d')
    return True
  except:
    return False

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

def getweekfirstday(dt):
  try:
    return dt + datetime.timedelta(days=1-dt.isoweekday())
  except:
    return None

def getweeklastday(dt):
  try:
    return dt + datetime.timedelta(days=7-dt.isoweekday())
  except:
    return None

def isweekend(s):
  try:
    return datetime.datetime.strptime(s,'%Y%m%d').isoweekday() == 7
  except:
    return False

def ismonthend(s):
  try:
    return (datetime.datetime.strptime(s,'%Y%m%d') + datetime.timedelta(days=1)).day == 1
  except:
    return False

def delbom(s):
  if s[0]==codecs.BOM_UTF8:
    return s[1:]
  else:
    return s

def delctrlchr(s, cc = '\n\t\r'):
  return str(s).translate(string.maketrans(cc,' '*len(cc)))

def sqlEscape(s):
  try:
    ret = str(s).replace('\\','\\\\')
    ret = ret.replace('`','\\`')
    return ret
  except:
    return None

def runHiveQL(s):
  logger.debug("sql# %s" % s)
  cmd = 'hive -S -e "%(sql)s"' % {'sql':sqlEscape(s)}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  hiveout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("HiveError!!!(%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return hiveout

def getscore(t):
  return int(t.get('topicnum',0)*WEIGHTS[c_topicnum][c_default] + \
             t.get('commentnum',0)*WEIGHTS[c_commentnum][c_default] + \
             t.get('startopicnum',0)*WEIGHTS[c_startopicnum][c_default] + \
             t.get('isnewuser',0)*WEIGHTS[c_isnewuser][c_default]
            )

reload(sys)
sys.setdefaultencoding('utf8')

retval = 0

##运行时变量
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
logdir = rundir + '/log'
tmpdir =  rundir + '/tmp'
if not os.path.exists(logdir):
  os.mkdir(logdir,0777)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0777)
logfile = '%(dir)s%(sep)s%(filename)s.log' % {'dir':logdir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

##日志器
logger = logging.getLogger('task')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(logfile)
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(levelname)s - %(message)s"))
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(filename)s\n%(message)s"))
logger.addHandler(consoleHandler)

logger.info("begin execute... %s" % str(sys.argv))

##参数解析
usageinfo = "%prog [--date=statisdate] [--post] [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.0")
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,'%Y%m%d')+datetime.timedelta(days=-1)).strftime('%Y%m%d'))
parser.add_option('-d', '--date', dest='statisdate', help='statis date, yyyy-mm-dd or yyyymmdd', metavar='DATE')
parser.add_option('-p', '--post', action='store_true', dest='postmode', default=False, help='post mode', metavar='MODE')
parser.add_option('-v', '--verbose', action='store_true', dest='verbosemode', default=False, help='verbose mode', metavar='MODE')

(options, args) = parser.parse_args()
statisdate = options.statisdate.replace('-','')
postmode = options.postmode
verbosemode = options.verbosemode

#参数检查
if verbosemode:
  consoleHandler.setLevel(logging.DEBUG)

if not isdate(statisdate):
  logger.error("unconverted date %s" % statisdate)
  sys.exit(-101)

#日期变量
dt_statisdate = datetime.datetime.strptime(statisdate,'%Y%m%d')
statis_date = dt_statisdate.strftime('%Y-%m-%d')
statis_week = getweekfirstday(dt_statisdate).strftime('%Y-%W')
statisweek_firstday = getweekfirstday(dt_statisdate).strftime('%Y-%m-%d')
statisweek_lastday = getweeklastday(dt_statisdate).strftime('%Y-%m-%d')
preday90_date = (dt_statisdate+datetime.timedelta(days=-90)).strftime("%Y-%m-%d")

#指标权重系数配置
c_topicnum, c_commentnum, c_startopicnum, c_isnewuser = range(4)
INDEX_FACTOR = [2.0, 1.0, 0.5, 50.0]
c_default, c_app, c_web = range(3)
SOURCE_FACTOR = [1.0, 1.2, 0.8]

WEIGHTS = []
for index in INDEX_FACTOR:
  row = []
  for source in SOURCE_FACTOR:
    row.append(index*source)
  WEIGHTS.append(row)

resultset = {}

#数据来源
logger.debug("connecting mysql ...")
dbconn = MySQLdb.connect(host='10.0.10.85',user='bi',passwd='bi_haodou',port=3306,charset='utf8')
sqlcursor = dbconn.cursor()

#获取小组管理员。小组管理员不纳入小组成员排名
sqlstmt = r"""
select GroupAdmins as admin1, ManageMaster as admin2, ManageSlave as admin3
from haodou_center.GroupCate
where parentid!=0
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

ignoreusers = []
for (admin1, admin2, admin3) in dataset:
  if admin1 is not None:
    admin = phpserialize.loads(admin1.encode('utf-8'))
    for item in admin:
      if int(item) not in ignoreusers:
        ignoreusers.append(int(item))
  if admin2 is not None:
    admin = phpserialize.loads(admin2.encode('utf-8'))
    for item in admin:
      if int(item) not in ignoreusers:
        ignoreusers.append(int(item))
  if admin3 is not None:
    admin = phpserialize.loads(admin3.encode('utf-8'))
    for item in admin:
      if int(item) not in ignoreusers:
        ignoreusers.append(int(item))
logger.debug("ignoreusers: %s" % ignoreusers)

#获取新用户。3个月内注册
sqlstmt = r"""
select userid
from haodou_passport.User
where status=1 and regtime>'%(preday90_date)s 00:00:00' 
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday, \
'preday90_date':preday90_date \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

newusers = []
for row in dataset:
  newusers.append(row[0])

#计算回复指标
sqlstmt = r"""
select ut.userid as userid, 
count(uc.commentid) as commentnum,
count(case when uc.sourceid=0 then uc.commentid end) as commentnum_web,
count(case when uc.sourceid in (1,2) then uc.commentid end) as commentnum_app
from 
(select commentid, userid, itemid as topicid, Platform as sourceid
from haodou_comment.Comment
where createtime between '%(statisweek_firstday)s 00:00:00' and '%(statisweek_lastday)s 23:59:59'
and type = '6' and status = '1'
) uc, 
haodou_center.GroupTopic ut 
where uc.topicid=ut.topicid
group by ut.userid
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, commentnum, commentnum_web, commentnum_app) in dataset:
  if userid in ignoreusers:
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['commentnum'] = int(commentnum)
  resultset[userid]['commentnum_web'] = int(commentnum_web)
  resultset[userid]['commentnum_app'] = int(commentnum_app)
  resultset[userid]['isnewuser'] = 1 if userid in newusers else 0

#计算话题指标
sqlstmt = r"""
select 
userid, 
count(topicid) as topicnum,
count(case when digest=1 or recommend=1 then topicid end) as startopicnum
from haodou_center.GroupTopic
where createtime between '%(statisweek_firstday)s 00:00:00' and '%(statisweek_lastday)s 23:59:59'
and status='1'
group by userid
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, topicnum, startopicnum) in dataset:
  if userid in ignoreusers:
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['topicnum'] = int(topicnum)
  resultset[userid]['startopicnum'] = int(startopicnum)
  resultset[userid]['isnewuser'] = 1 if userid in newusers else 0

sqlcursor.close()
dbconn.commit()
dbconn.close()

#计算活跃度得分
for item in resultset:
  resultset[item]['score'] = getscore(resultset[item])

#计算排名（按活跃度得分从大到小排序）
resultusers, resultscores, top = [], [], 50
sortset = [(resultset[item]['score'], resultset[item]['userid']) for item in resultset]
sortset.sort(reverse=True)
for (index, item) in enumerate(sortset):
  resultset[item[1]]['sn'] = index+1
  if top==0 or index<top:
    resultusers.append(str(item[1]))
    resultscores.append(str(item[0]))

#推送第1次
if postmode:
  logger.debug("sending data (1)...")
  #简化结果集（格式与推送接口约定保持一致）
  simpleset, top = {}, 50
  simpleset['gid'] = 'user_rank'
  simpleset['users'] = []
  for item in resultset:
    if top>0 and resultset[item]['sn']>top:
      continue
    simpleitem = {}
    simpleitem['uid'] = resultset[item]['userid']
    simpleitem['rank'] = resultset[item]['sn']
    simpleitem['score'] = resultset[item]['score']
    simpleset['users'].append(simpleitem)
  simpledata = json.dumps(simpleset)
  #推送
  posturl = 'http://211.151.151.230/data/userrank'
  httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
  headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
  postdata = simpledata
  logger.info("postdata: %s" % postdata)
  try:
    httpconn.request(method='POST',url=urlparse(posturl).path,body=postdata,headers=headers);
    httpresp = httpconn.getresponse()
    httpstatus, httpreason, httptext = httpresp.status, httpresp.reason, httpresp.read()
    httpconn.close()
    if httpresp.status!=httplib.OK:
      logger.error('数据发送失败!(1)')
      logger.error("Debug Info: %s %s - %s" % (httpstatus, httpreason, httptext))
  except (httplib.HTTPException, socket.error) as ex:
    logger.error("网络错误:%s" % ex)

#推送第2次
if postmode:
  logger.debug("sending data (2)...")
  posturl = 'http://211.151.151.230/data/top'
  httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
  headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
  postdata = "category=user&type=grpactive&cache=0&ids=%(users)s&counts=%(scores)s" % {'url':urlparse(posturl).path, 'users':','.join(resultusers), 'scores':','.join(resultscores)}
  posturl = "%(url)s?%(data)s" % {'url':urlparse(posturl).path, 'data':postdata}
  logger.info("postdata: %s" % postdata)
  try:
    httpconn.request(method='POST',url=posturl,body=postdata,headers=headers);
    httpresp = httpconn.getresponse()
    httpstatus, httpreason, httptext = httpresp.status, httpresp.reason, httpresp.read()
    httpconn.close()
    if httpresp.status!=httplib.OK:
      logger.error('数据发送失败!(2)')
      logger.error("Debug Info: %s %s - %s" % (httpstatus, httpreason, httptext))
  except (httplib.HTTPException, socket.error) as ex:
    logger.error("网络错误:%s" % ex)

#输出结果集格式定义（与结果表bing.rpt_grp_weekactrank_dm 结构一致）
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = \
  "%(user_id)s" + fieldsdelimiter + \
  "%(topic_num)d" + fieldsdelimiter + \
  "%(startopic_num)d" + fieldsdelimiter + \
  "%(comment_num)d" + fieldsdelimiter + \
  "%(commentnum_web)d" + fieldsdelimiter + \
  "%(commentnum_app)d" + fieldsdelimiter + \
  "%(score)d" + fieldsdelimiter + \
  "%(sn)d" + rowsdelimiter

#输出结果集文件
logger.debug("writing file ...")
resultchunk, top = [], 0
for item in resultset:
  if top>0 and resultset[item]['sn']>top:
    continue
  resultline = resultrowformat % { \
    'user_id':resultset[item]['userid'], \
    'topic_num':resultset[item].get('topicnum',0), \
    'startopic_num':resultset[item].get('startopicnum',0), \
    'comment_num':resultset[item].get('commentnum',0), \
    'commentnum_web':resultset[item].get('commentnum_web',0), \
    'commentnum_app':resultset[item].get('commentnum_app',0), \
    'score':resultset[item]['score'], \
    'sn':resultset[item]['sn'] \
    }
  resultchunk.append(resultline)
tmpfilename = "rpt_grp_memberweekactrank_dm_%(statisdate)s.dat" % {'statisdate':statisdate}
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#加载入Hive表
logger.debug("loading hive ...")
runHiveQL("load data local inpath '%(tmpfile)s' overwrite into table bing.rpt_grp_memberweekactrank_dm partition (statis_date='%(statis_date)s');" % {'tmpfile':tmpfile, 'statis_date':statis_date})

#为展现附加一个提供了名字的hive表
sqlstmt = r"""
set hive.auto.convert.join=false;

insert overwrite table bing.rpt_grp_memberweekactrank_dm_name partition (statis_date='%(statis_date)s')
select 
t.user_id,
u.username,
t.topic_num,
t.startopic_num,
t.comment_num,
t.commentnum_web,
t.commentnum_app,
t.score,
t.sn
from (select * from bing.rpt_grp_memberweekactrank_dm where statis_date='%(statis_date)s') t
inner join haodou_passport_%(rundate)s.`User` u on (t.user_id=u.userid)
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statis_date':statis_date \
}
runHiveQL(sql)

logger.info("end.(%d)" % retval)
sys.exit(retval)
