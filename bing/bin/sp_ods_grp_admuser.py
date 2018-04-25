#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import logging
from optparse import OptionParser
import string, codecs
import subprocess
import MySQLdb
import json, phpserialize
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
logger = logging.getLogger('bing_sp')
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
usageinfo = "%prog [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.0")
parser.add_option('-v', '--verbose', action='store_true', dest='verbosemode', default=False, help='verbose mode', metavar='MODE')

(options, args) = parser.parse_args()
verbosemode = options.verbosemode

#参数检查
if verbosemode:
  consoleHandler.setLevel(logging.DEBUG)

#日期变量
statisdate=(datetime.datetime.strptime(rundate,'%Y%m%d')+datetime.timedelta(days=-1)).strftime('%Y%m%d')
dt_statisdate = datetime.datetime.strptime(statisdate,'%Y%m%d')
statis_date = dt_statisdate.strftime('%Y-%m-%d')
statis_week = getweekfirstday(dt_statisdate).strftime('%Y-%W')
statisweek_firstday = getweekfirstday(dt_statisdate).strftime('%Y-%m-%d')
statisweek_lastday = getweeklastday(dt_statisdate).strftime('%Y-%m-%d')

jobname = "%(script)s@%(statisdate)s" % {'script':os.path.split(os.path.abspath(__file__))[1], 'statisdate':statisdate}
hiveconf = {'mapred.job.name':jobname, 'mapred.job.queue.name':'default'}
conf = ''
for item in hiveconf:
  conf += '--hiveconf %(key)s=%(value)s ' % {'key':item, 'value':hiveconf[item]}
bin = "hive -S %(hiveconf)s -e" % {'rundir':rundir, 'hiveconf':conf}

#结果集
resultset = {}

#当前数据来源
logger.debug("connecting mysql ...")
dbconn = MySQLdb.connect(host='10.0.10.85',user='bi',passwd='bi_haodou',port=3306,charset='utf8')
sqlcursor = dbconn.cursor()

#从mysql中获取当前生活盟主
sqlstmt = r"""
select
v.userid, v.username
from haodou_center.VipUser v
where v.viptype='1' and v.status='1'
and v.userid in (select userid
from haodou_center.VipUserTag 
where tagid in (10043,10060))
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

for (userid, username) in dataset:
  userid = int(userid)
  if userid not in resultset:
    resultset[userid] = {}
    resultset[userid]['userid']      = userid
    resultset[userid]['username']    = username
    resultset[userid]['group_name']  = u'广场'
    resultset[userid]['group_title'] = u'生活盟主'
    resultset[userid]['eff_date']    = statis_date
    resultset[userid]['exp_date']    = '2020-01-01'
    resultset[userid]['status']      = 1

#从mysql中获取当前小组管理员 排除16:站务帮助/17:公告/26:生活品牌馆/28:豆友会
sqlstmt = r"""
select `Name` as group_name, GroupAdmins as admin1, ManageMaster as admin2, ManageSlave as admin3
from haodou_center.GroupCate
where cateid not in (16,17,26,28) and parentid!=0
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

for (group_name, admin1, admin2, admin3) in dataset:
  if admin1 is not None:
    admin = phpserialize.loads(admin1.encode('utf-8'))
    for item in admin:
      userid = int(item)
      username = admin[item].decode('UTF-8')
      if userid not in resultset:
        resultset[userid] = {}
        resultset[userid]['userid']      = userid
        resultset[userid]['username']    = username
        resultset[userid]['group_name']  = group_name
        resultset[userid]['group_title'] = u'小组组长'
        resultset[userid]['eff_date']    = statis_date
        resultset[userid]['exp_date']    = '2020-01-01'
        resultset[userid]['status']      = 1
  if admin2 is not None:
    admin = phpserialize.loads(admin2.encode('utf-8'))
    for item in admin:
      userid = int(item)
      username = admin[item].decode('UTF-8')
      if userid not in resultset:
        resultset[userid] = {}
        resultset[userid]['userid']      = userid
        resultset[userid]['username']    = username
        resultset[userid]['group_name']  = group_name
        resultset[userid]['group_title'] = u'小组组长'
        resultset[userid]['eff_date']    = statis_date
        resultset[userid]['exp_date']    = '2020-01-01'
        resultset[userid]['status']      = 1
  if admin3 is not None:
    admin = phpserialize.loads(admin3.encode('utf-8'))
    for item in admin:
      userid = int(item)
      username = admin[item].decode('UTF-8')
      if userid not in resultset:
        resultset[userid] = {}
        resultset[userid]['userid']      = userid
        resultset[userid]['username']    = username
        resultset[userid]['group_name']  = group_name
        resultset[userid]['group_title'] = u'小组组长'
        resultset[userid]['eff_date']    = statis_date
        resultset[userid]['exp_date']    = '2020-01-01'
        resultset[userid]['status']      = 1

sqlcursor.close()
dbconn.commit()
dbconn.close()

#从hive中获取历史数据与当前数据合并
##查询输出数据格式
outfieldnum=7
c_userid     , \
c_username   , \
c_group_name , \
c_group_title, \
c_eff_date   , \
c_exp_date   , \
c_status = range(outfieldnum)

sqlstmt = R"""
set hive.cli.print.header=false;
select userid, username, group_name, group_title, eff_date, exp_date, status
from bing.ods_grp_admuser
;
"""
sql = delctrlchr(sqlstmt,'\t\r')
logger.debug("sql# %s" % sql)
cmd = '%(bin)s "%(sql)s"' % {'bin':bin, 'sql':sqlEscape(sql)}
hive = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

rownum, errnum = 0, 0
for outline in hive.stdout:
  if outline[:5]=="WARN:":
    continue
  outrow = outline.strip().split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  rownum += 1
  userid = int(outrow[c_userid])
  if userid not in resultset:
    resultset[userid] = {}
    resultset[userid]['userid']      = userid
    resultset[userid]['username']    = outrow[c_username]
    resultset[userid]['group_name']  = outrow[c_group_name]
    resultset[userid]['group_title'] = outrow[c_group_title]
    resultset[userid]['eff_date']    = outrow[c_eff_date]
    resultset[userid]['exp_date']    = statis_date
    resultset[userid]['status']      = 0
  else:
    resultset[userid]['eff_date']    = outrow[c_eff_date]

init_flag = True if rownum==0 else False

#将处理完的数据载入hive
#输出结果集格式定义（与结果表bing.ods_grp_admuser 结构一致）
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = \
  "%(userid)d" + fieldsdelimiter + \
  "%(username)s" + fieldsdelimiter + \
  "%(group_name)s" + fieldsdelimiter + \
  "%(group_title)s" + fieldsdelimiter + \
  "%(eff_date)s" + fieldsdelimiter + \
  "%(exp_date)s" + fieldsdelimiter + \
  "%(status)d" + rowsdelimiter

#输出结果集文件
logger.debug("writing file ...")
resultchunk = []
for item in resultset:
  resultline = resultrowformat % { \
    'userid':resultset[item]['userid'], \
    'username':resultset[item]['username'], \
    'group_name':resultset[item]['group_name'], \
    'group_title':resultset[item]['group_title'], \
    'eff_date':resultset[item]['eff_date'] if not init_flag else '2010-01-01', \
    'exp_date':resultset[item]['exp_date'], \
    'status':resultset[item]['status'] \
    }
  resultchunk.append(resultline)
tmpfilename = "ods_grp_admuser_%(statisdate)s.dat" % {'statisdate':statisdate}
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#载入Hive表
logger.debug("loading hive ...")
runHiveQL("load data local inpath '%(tmpfile)s' overwrite into table bing.ods_grp_admuser;" % {'tmpfile':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)
