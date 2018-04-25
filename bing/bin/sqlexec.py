#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import logging
from optparse import OptionParser
import math
import string, codecs
import subprocess

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),"%Y%m%d")
    return True
  except:
    return False

def mklogfile(s):
  if not os.path.exists(s):
    f=open(s,'w')
    f.write('.log\n')
    f.close()
    os.chmod(s, 0664)

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
    return datetime.datetime.strptime(s,"%Y%m%d").isoweekday() == 7
  except:
    return False

def ismonthend(s):
  try:
    return (datetime.datetime.strptime(s,"%Y%m%d") + datetime.timedelta(days=1)).day == 1
  except:
    return False

def delbom(s):
  if s[0]==codecs.BOM_UTF8:
    return s[1:]
  else:
    return s

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

def sqlprepare(s):
  try:
    return str(s)
  except:
    return None

def sqlescape(s):
  try:
    ret = str(s).replace('\\','\\\\')
    ret = ret.replace('`','\\`')
    return ret
  except:
    return None

reload(sys)
sys.setdefaultencoding("utf8")

##runtime variable
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
confile = '%(dir)s%(sep)s%(filename)s.conf' % {'dir':rundir,'sep':os.sep,'filename':runfilename}
homedir=os.path.dirname(os.path.abspath(__file__)) + "/.." # remove bin path
logdir = homedir + "/log"
tmpdir =  homedir + "/tmp"
if not os.path.exists(logdir):
  os.mkdir(logdir,0774)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0774)
logfile = '%(dir)s%(sep)s%(filename)s.log' % {'dir':logdir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

##logger
logger = logging.getLogger("sqlexec")
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

##option parse
usageinfo = "%prog --sql=sqlfile [--date=statisdate] [--period=day/week/month] [--exec=hive/spark] [-v] [-n]"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.2")
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,"%Y%m%d")+datetime.timedelta(days=-1)).strftime("%Y%m%d"))
parser.add_option("-s", "--sql", dest="sqlfile", help="sql file", metavar="FILE")
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")
parser.add_option("-p", "--period", dest="periodmode", default="day", help="period mode, day or week or month", metavar="STRING")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")
parser.add_option("-n", "--justprint", action="store_true", dest="printmode", default=False, help="just-print mode", metavar="MODE")
parser.add_option("-x", "--xid", dest="xid", default="null", help="xid", metavar="VALUE")
parser.add_option("-e", "--exec", dest="executor", default="hive", help="sql executor, hive or spark", metavar="VALUE")

(options, args) = parser.parse_args()
sqlfile = options.sqlfile
statisdate = options.statisdate.replace('-','')
periodmode = options.periodmode.lower()[0]
verbosemode = options.verbosemode
printmode = options.printmode
executor = options.executor.lower()
xid = options.xid

if verbosemode==True or printmode==True:
  consoleHandler.setLevel(logging.DEBUG)

if sqlfile==None:
  logger.error("SQL file is missing.")
  sys.exit(-101)

if os.path.isfile(sqlfile)==False:
  logger.error("SQL file is missing. %s" % sqlfile)
  sys.exit(-102)

if os.access(sqlfile,os.R_OK)==False:
  logger.error("SQL file is not readable. %s" % sqlfile)
  sys.exit(-103)

if isdate(statisdate)==False:
  logger.error("unconverted date %s" % statisdate)
  sys.exit(-104)

if periodmode=="w" and not isweekend(statisdate):
  logger.warning("%s is not the weekend." % statisdate)
  sys.exit(0)

if periodmode=="m" and not ismonthend(statisdate):
  logger.warning("%s is not the last day of the month." % statisdate)
  sys.exit(0)

if executor=="hive":
  jobname = "%(script)s@%(statisdate)s" % {'script':os.path.split(os.path.abspath(sqlfile))[1], 'statisdate':statisdate}
  hiveconf = {'mapred.job.name':jobname, 'mapred.job.queue.name':'default'}
  conf = ''
  for item in hiveconf:
    conf += '--hiveconf %(key)s=%(value)s ' % {'key':item, 'value':hiveconf[item]}
  bin = "hive -S %(hiveconf)s -i %(rundir)s/sqlexec.rc -e" % {'rundir':rundir, 'hiveconf':conf}
elif executor=="spark":
  bin = "spark-sql -S -i %(rundir)s/sqlexec.rc -e" % {'rundir':rundir}
else:
  logger.error("executor:%s not supported." % executor)
  sys.exit(-100)

dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
##sql variable
statis_date = dt_statisdate.strftime("%Y-%m-%d")
statisdate = dt_statisdate.strftime("%Y%m%d")
statis_begin_time = "%s %s" % (statis_date, str(datetime.time.min))
statis_end_time = "%s %s" % (statis_date, str(datetime.time.max))
preday_date = (dt_statisdate+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
preday2_date = (dt_statisdate+datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
preday3_date = (dt_statisdate+datetime.timedelta(days=-3)).strftime("%Y-%m-%d")
preday4_date = (dt_statisdate+datetime.timedelta(days=-4)).strftime("%Y-%m-%d")
preday5_date = (dt_statisdate+datetime.timedelta(days=-5)).strftime("%Y-%m-%d")
preday6_date = (dt_statisdate+datetime.timedelta(days=-6)).strftime("%Y-%m-%d")
preday7_date = (dt_statisdate+datetime.timedelta(days=-7)).strftime("%Y-%m-%d")
preday13_date = (dt_statisdate+datetime.timedelta(days=-13)).strftime("%Y-%m-%d")
preday14_date = (dt_statisdate+datetime.timedelta(days=-14)).strftime("%Y-%m-%d")
preday29_date = (dt_statisdate+datetime.timedelta(days=-29)).strftime("%Y-%m-%d")
preday30_date = (dt_statisdate+datetime.timedelta(days=-30)).strftime("%Y-%m-%d")
preday60_date = (dt_statisdate+datetime.timedelta(days=-60)).strftime("%Y-%m-%d")
nextday_date = (dt_statisdate+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
nextdaydate = (dt_statisdate+datetime.timedelta(days=1)).strftime("%Y%m%d")
nextday30_date = (dt_statisdate+datetime.timedelta(days=30)).strftime("%Y-%m-%d")
nextday60_date = (dt_statisdate+datetime.timedelta(days=60)).strftime("%Y-%m-%d")
statis_month = dt_statisdate.strftime("%Y-%m")
statis_year = dt_statisdate.strftime("%Y")
firstday_date = dt_statisdate.strftime("%Y-%m-01")
if dt_statisdate.month==12:
  lastday_date = dt_statisdate.strftime("%Y-12-31")
else:
  lastday_date = (datetime.date(dt_statisdate.year,dt_statisdate.month+1,1)+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
statis_week = getweekfirstday(dt_statisdate).strftime("%Y-%W")
statisweek_firstday = getweekfirstday(dt_statisdate).strftime("%Y-%m-%d")
statisweek_lastday = getweeklastday(dt_statisdate).strftime("%Y-%m-%d")
preweek = getweekfirstday(datetime.datetime.strptime(preday7_date,"%Y-%m-%d")).strftime("%Y-%W")
curdate = rundate
cur_date = "%s-%s-%s" % (rundate[0:4],rundate[4:6],rundate[6:8])

##read sql file
sqlhandle = codecs.open(sqlfile,'r','utf-8')
sqlstmt = sqlhandle.read()
sqlhandle.close()
sqlstmt = delbom(sqlstmt)

##sql prepare
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql.replace('${statis_date}',statis_date)
sql = sql.replace('${statisdate}',statisdate)
sql = sql.replace('${statis_begin_time}',statis_begin_time)
sql = sql.replace('${statis_end_time}',statis_end_time)
sql = sql.replace('${preday_date}',preday_date)
sql = sql.replace('${preday2_date}',preday2_date)
sql = sql.replace('${preday3_date}',preday3_date)
sql = sql.replace('${preday4_date}',preday4_date)
sql = sql.replace('${preday5_date}',preday5_date)
sql = sql.replace('${preday6_date}',preday6_date)
sql = sql.replace('${preday7_date}',preday7_date)
sql = sql.replace('${preday13_date}',preday13_date)
sql = sql.replace('${preday14_date}',preday14_date)
sql = sql.replace('${preday29_date}',preday29_date)
sql = sql.replace('${preday30_date}',preday30_date)
sql = sql.replace('${preday60_date}',preday60_date)
sql = sql.replace('${nextday_date}',nextday_date)
sql = sql.replace('${nextdaydate}',nextdaydate)
sql = sql.replace('${nextday30_date}',nextday30_date)
sql = sql.replace('${nextday60_date}',nextday60_date)
sql = sql.replace('${statis_month}',statis_month)
sql = sql.replace('${statis_year}',statis_year)
sql = sql.replace('${firstday_date}',firstday_date)
sql = sql.replace('${lastday_date}',lastday_date)
sql = sql.replace('${statis_week}',statis_week)
sql = sql.replace('${statisweek_firstday}',statisweek_firstday)
sql = sql.replace('${statisweek_lastday}',statisweek_lastday)
sql = sql.replace('${preweek}',preweek)
sql = sql.replace('${cur_date}',cur_date)
sql = sql.replace('${curdate}',curdate)
sql = sql.replace('${xid}',xid)
logger.debug("sql = %s" % sql)

if printmode==True:
  logger.info("print end.")
  sys.exit(0)

##set session env
os.environ["SHARK_MASTER_MEM"]="10g"
os.environ["SHARK_MEM"]="10g"

##execute sql
cmd = '%(bin)s "%(sql)s"' % {'bin':bin, 'sql':sqlescape(sql)}
proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
retmsg, errmsg = proc.communicate()
logger.debug("%s" % str(retmsg))
retval = proc.wait()
if retval!=0:
  logger.error("CalledProcessError!!!(%s,%d)" % (executor, retval))
  logger.error("Debug Info: %s" % str(errmsg))
  sys.exit(retval)

logger.info("execute end.(%d)" % retval)
sys.exit(retval)
