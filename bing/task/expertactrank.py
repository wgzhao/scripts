#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import logging
from optparse import OptionParser
import string, codecs
import subprocess
import MySQLdb
import json
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

def getvipscore(t):
  return int(t.get('recipenum',0)*VIP_WEIGHTS[c_recipenum] + \
             t.get('photonum',0)*VIP_WEIGHTS[c_photonum] + \
             t.get('topicnum',0)*VIP_WEIGHTS[c_topicnum]
            )

def getlifescore(t):
  return int(t.get('topicnum',0)*LIFE_WEIGHTS[c_topicnum] + \
             t.get('commentnum',0)*LIFE_WEIGHTS[c_commentnum] + \
             t.get('startopicnum',0)*LIFE_WEIGHTS[c_startopicnum]
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
preday7_date = (dt_statisdate+datetime.timedelta(days=-7)).strftime("%Y-%m-%d")
preday14_date = (dt_statisdate+datetime.timedelta(days=-14)).strftime("%Y-%m-%d")
preday30_date = (dt_statisdate+datetime.timedelta(days=-30)).strftime("%Y-%m-%d")
statis_week = getweekfirstday(dt_statisdate).strftime('%Y-%W')
statisweek_firstday = getweekfirstday(dt_statisdate).strftime('%Y-%m-%d')
statisweek_lastday = getweeklastday(dt_statisdate).strftime('%Y-%m-%d')

#指标权重系数配置
c_recipenum, c_photonum, c_topicnum, c_commentnum, c_startopicnum = range(5)
VIP_WEIGHTS  = [2.0, 1.0, 0.5, 0.0, 0.0]
LIFE_WEIGHTS = [0.0, 0.0, 2.0, 1.0, 0.8]

resultset = {}

#数据来源
logger.debug("connecting mysql ...")
dbconn = MySQLdb.connect(host='10.0.10.85',user='bi',passwd='bi_haodou',port=3306,charset='utf8')
sqlcursor = dbconn.cursor()

#加载达人
logger.debug("loading vip user ...")
sqlstmt = r"""
select distinct v.userid
from haodou_center.VipUser v,
haodou_center.VipUserTag t
where v.viptype=1 and v.status=1 and v.userid=t.userid 
and t.tagid not in (10038,10043,10056,10060) and t.tagid <10070
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()
vipusers = []
for item in dataset:
  vipusers.append(int(item[0]))
  if item[0] not in resultset:
    resultset[item[0]] = {}
  resultset[item[0]]['statis_date'] = statis_date
  resultset[item[0]]['userid'] = int(item[0])
logger.debug("vip: %s" % str(vipusers))

#加载生活盟主
logger.debug("loading life user ...")
sqlstmt = r"""
select distinct v.userid
from haodou_center.VipUser v,
haodou_center.VipUserTag t
where v.viptype=1 and v.status=1 and v.userid=t.userid 
and t.tagid in (10043,10060)
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()
lifeusers = []
for item in dataset:
  lifeusers.append(int(item[0]))
  if item[0] not in resultset:
    resultset[item[0]] = {}
  resultset[item[0]]['statis_date'] = statis_date
  resultset[item[0]]['userid'] = int(item[0])
logger.debug("vip: %s" % str(lifeusers))

#计算菜谱指标
sqlstmt = r"""
select userid, count(recipeid) as recipenum
from haodou_recipe.Recipe
where reviewtime between '%(statis_date)s 00:00:00' and '%(statis_date)s 23:59:59'
and status=0
group by userid
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, recipenum) in dataset:
  if not (userid in vipusers or userid in lifeusers):
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['recipenum'] = int(recipenum)

#计算作品指标
sqlstmt = r"""
select userid, count(id) as photonum
from haodou_photo.Photo
where createtime between '%(statis_date)s 00:00:00' and '%(statis_date)s 23:59:59'
and status=1
group by userid
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, photonum) in dataset:
  if not (userid in vipusers or userid in lifeusers):
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['photonum'] = int(photonum)

#计算话题指标
sqlstmt = r"""
select 
userid, 
count(topicid) as topicnum,
count(case when digest=1 or recommend=1 then topicid end) as startopicnum
from haodou_center.GroupTopic
where createtime between '%(statis_date)s 00:00:00' and '%(statis_date)s 23:59:59'
and status=1
group by userid
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql % {'rundate':rundate, \
'statisdate':statisdate, \
'statis_date':statis_date, \
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, topicnum, startopicnum) in dataset:
  if not (userid in vipusers or userid in lifeusers):
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['topicnum'] = int(topicnum)
  resultset[userid]['startopicnum'] = int(startopicnum)

#计算回复指标
sqlstmt = r"""
select ut.userid as userid, 
count(uc.commentid) as commentnum
from 
(select commentid, userid, itemid as topicid
from haodou_comment.Comment
where createtime between '%(statis_date)s 00:00:00' and '%(statis_date)s 23:59:59'
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
'preday7_date':preday7_date, \
'preday14_date':preday14_date, \
'preday30_date':preday30_date, \
'statis_week':statis_week, \
'statisweek_firstday':statisweek_firstday, \
'statisweek_lastday':statisweek_lastday \
}
logger.debug("sql# %s" % sql)
sqlcursor.execute(sql)
dataset = sqlcursor.fetchall()

for (userid, commentnum) in dataset:
  if not (userid in vipusers or userid in lifeusers):
    continue
  if userid not in resultset:
    resultset[userid] = {}
  resultset[userid]['statis_date'] = statis_date
  resultset[userid]['userid'] = int(userid)
  resultset[userid]['commentnum'] = int(commentnum)

sqlcursor.close()
dbconn.commit()
dbconn.close()

#计算达人/生活盟主活跃度得分
for item in resultset:
  resultset[item]['vipscore'] = -1
  resultset[item]['lifescore'] = -1
  if resultset[item]['userid'] in vipusers:
    resultset[item]['vipscore'] = getvipscore(resultset[item])
  if resultset[item]['userid'] in lifeusers:
    resultset[item]['lifescore'] = getlifescore(resultset[item])

logger.debug(resultset)

#计算达人排名（按达人活跃度得分从大到小排序）
resultvipusers, resultvipscores, top = [], [], 0
sortset = [(resultset[item]['vipscore'], resultset[item]['userid']) for item in resultset]
sortset.sort(reverse=True)
for (index, item) in enumerate(sortset):
  resultset[item[1]]['vipsn'] = index+1
  if resultset[item[1]]['userid'] not in vipusers:
    continue
  if top==0 or index<top:
    resultvipusers.append(str(item[1]))
    resultvipscores.append(str(item[0]))

logger.debug("vipusers: %s" % resultvipusers)
logger.debug("vipscores: %s" % resultvipscores)

#计算生活盟主排名（按生活盟主活跃度得分从大到小排序）
resultlifeusers, resultlifescores, top = [], [], 0
sortset = [(resultset[item]['lifescore'], resultset[item]['userid']) for item in resultset]
sortset.sort(reverse=True)
for (index, item) in enumerate(sortset):
  resultset[item[1]]['lifesn'] = index+1
  if resultset[item[1]]['userid'] not in lifeusers:
    continue
  if top==0 or index<top:
    resultlifeusers.append(str(item[1]))
    resultlifescores.append(str(item[0]))

logger.debug("lifeusers: %s" % resultlifeusers)
logger.debug("lifescores: %s" % resultlifescores)

#推送达人数据
if postmode:
  logger.debug("sending data (1)...")
  posturl = 'http://211.151.151.230/data/top'
  httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
  headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
  postdata = "category=user&type=rcpexpert&cache=0&ids=%(users)s&counts=%(scores)s" % {'url':urlparse(posturl).path, 'users':','.join(resultvipusers), 'scores':','.join(resultvipscores)}
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

#推送生活盟主数据
if postmode:
  logger.debug("sending data (2)...")
  posturl = 'http://211.151.151.230/data/top'
  httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
  headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
  postdata = "category=user&type=lifeexpert&cache=0&ids=%(users)s&counts=%(scores)s" % {'url':urlparse(posturl).path, 'users':','.join(resultlifeusers), 'scores':','.join(resultlifescores)}
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

#输出结果集格式定义（与结果表bing.rpt_grp_vipweekactrank_dm 结构一致）
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = \
  "%(user_id)s" + fieldsdelimiter + \
  "%(recipe_num)d" + fieldsdelimiter + \
  "%(photo_num)d" + fieldsdelimiter + \
  "%(topic_num)d" + fieldsdelimiter + \
  "%(startopic_num)d" + fieldsdelimiter + \
  "%(comment_num)d" + fieldsdelimiter + \
  "%(vip_score)d" + fieldsdelimiter + \
  "%(life_score)d" + fieldsdelimiter + \
  "%(vip_sn)d" + fieldsdelimiter + \
  "%(life_sn)d" + rowsdelimiter

#输出结果集文件
logger.debug("writing file ...")
resultchunk, top = [], 0
for item in resultset:
  if not (top==0 or resultset[item]['vipsn']<=top or resultset[item]['lifesn']<=top):
    continue
  resultline = resultrowformat % { \
    'user_id':resultset[item]['userid'], \
    'recipe_num':resultset[item].get('recipenum',0), \
    'photo_num':resultset[item].get('photonum',0), \
    'topic_num':resultset[item].get('topicnum',0), \
    'startopic_num':resultset[item].get('startopic_num',0), \
    'comment_num':resultset[item].get('commentnum',0), \
    'vip_score':resultset[item].get('vipscore',0), \
    'life_score':resultset[item].get('lifescore',0), \
    'vip_sn':resultset[item]['vipsn'], \
    'life_sn':resultset[item]['lifesn'] \
    }
  resultchunk.append(resultline)
tmpfilename = "rpt_grp_vipweekactrank_dm_%(statisdate)s.dat" % {'statisdate':statisdate}
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#加载入Hive表
logger.debug("loading hive ...")
runHiveQL("load data local inpath '%(tmpfile)s' overwrite into table bing.rpt_grp_vipweekactrank_dm partition (statis_date='%(statis_date)s');" % {'tmpfile':tmpfile, 'statis_date':statis_date})

logger.info("end.(%d)" % retval)
sys.exit(retval)
