#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import logging
from optparse import OptionParser
import string, codecs
import subprocess
import geoip2.database
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

def isipaddrv4(s):
  regexp_ip = '\d+\.\d+\.\d+\.\d+'
  match = re.match(regexp_ip, s)
  if match:
    return True
  return False

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

def isurl(url):
  if re.match(r'^https?:/{2}\w.+$', url):
    return True
  else:
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

def runHiveQL(sql):
  logger.debug("sql# %s" % sql)
  cmd = 'hive -S -e "%(sql)s"' % {'sql':sqlEscape(sql)}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  hiveout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("HiveError!!!(%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return hiveout

def cmdEscape(s):
  try:
    ret = str(s).replace("`","'")
    ret = ret.replace("'","\\'")
    return ret
  except:
    return None

def runShell(cmd):
  cmdline = cmdEscape(cmd)
  logger.debug("shell# %s" % cmdline)
  proc = subprocess.Popen(cmdline, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  shellout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("ShellError!!!(%d) %s" % (retval, cmdline))
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return shellout

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
usageinfo = "%prog --data=$datafile --url=$posturl [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.0")
parser.add_option('-d', '--data', dest='datafile', help='data file', metavar='FILE')
parser.add_option('-u', '--url', dest='posturl', help='post url', metavar='STRING')
parser.add_option('-v', '--verbose', action='store_true', dest='verbosemode', default=False, help='verbose mode', metavar='MODE')

(options, args) = parser.parse_args()
datafile = options.datafile
posturl = 'http://211.151.151.230/data/usrecomm'
verbosemode = options.verbosemode

#参数检查
if verbosemode:
  consoleHandler.setLevel(logging.DEBUG)

if datafile==None:
  logger.error("Data file is missing.")
  sys.exit(-101)

if os.path.isfile(datafile)==False:
  logger.error("Data file is missing. %s" % datafile)
  sys.exit(-102)

if os.access(datafile,os.R_OK)==False:
  logger.error("Data file is not readable. %s" % datafile)
  sys.exit(-103)

if isurl(posturl)==False:
  logger.error("Post Url Error. %s" % posturl)
  sys.exit(-104)

logger.debug("loading data ...")

filehandle = codecs.open(datafile,'r','utf-8')
filedata = filehandle.readlines()
filehandle.close()
filedata = delbom(filedata)

#推送
httperrnum, httperrlimit = 0, 3
posturl = 'http://211.151.151.230/data/usrecomm'
httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
for line in filedata:
  postdata = line.strip('\n')
  logger.debug("postdata: %s" % postdata)
  if httperrlimit>0 and httperrnum>=httperrlimit:
    continue
  try:
    httpconn.request(method='POST',url=urlparse(posturl).path,body=postdata,headers=headers);
    httpresp = httpconn.getresponse()
    httpstatus, httpreason, httptext = httpresp.status, httpresp.reason, httpresp.read()
    httpconn.close()
    if httpresp.status!=httplib.OK:
      logger.error("数据发送失败!%s" % cur_city)
      logger.error("Debug Info: %s %s - %s" % (httpstatus, httpreason, httptext))
      httperrnum += 1
  except (httplib.HTTPException, socket.error) as ex:
    logger.error("网络错误:%s" % ex)
    httperrnum += 1

logger.info("网络错误次数:%d/%d" % (httperrnum, httperrlimit))

logger.info("end.(%d)" % retval)
sys.exit(retval)
