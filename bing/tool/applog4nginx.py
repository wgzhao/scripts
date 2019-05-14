#! /usr/bin/env python
# -*- coding: utf-8 -*-

#在applog 日志文件生成失败的情况下，依据nginx 日志来补充生成applg 文件（缺失请求参数信息）。
#调用示例： ./applog4nginx.py -b "2015-09-21 14:38:00" -e "2015-09-22 17:56:59" -p 211.151.151.238 -v

import sys, os, datetime, time
import string, codecs, ujson
import logging
from optparse import OptionParser
import subprocess
import re
import urlparse
import struct, base64

def mklogfile(s):
  if not os.path.exists(s):
    f=open(s,'w')
    f.write('.log\n')
    f.close()
    os.chmod(s, 0666)

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),"%Y%m%d")
    return True
  except:
    return False

def istime(s):
  try:
    time.strptime(str(s),"%H:%M:%S")
    return True
  except:
    return False

def isdatetime(s):
  try:
    time.strptime(str(s),"%Y-%m-%d %H:%M:%S")
    return True
  except:
    return False

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

def isipv4(s):
  a = s.split('.')
  return len(a) == 4 and len(filter(lambda x: x >= 0 and x <= 255, \
         map(int, filter(lambda x: x.isdigit(), a)))) == 4

def cmdEscape(s):
  try:
    ret = str(s).replace("`","'").replace("'","\\'")
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

#todo 封装抽取过程
def transformApplog(nginxfile, starttime = None, endtime = None):
  applogfile = nginxfile
  return 0, applogfile

reload(sys)
sys.setdefaultencoding("utf8")

retval = 0

#运行时变量
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
logdir = rundir + "/log"
tmpdir =  rundir + "/tmp"
if not os.path.exists(logdir):
  os.mkdir(logdir,0777)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0777)
logfile = '%(dir)s/%(filename)s.log' % {'dir':logdir,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

#日志器
logger = logging.getLogger("nginx2applog")
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

#参数解析
usageinfo = "%prog [--date=statisdate] [-v]"
verinfo = "%prog v0.0.1"
parser = OptionParser(usage=usageinfo, version=verinfo)
parser.add_option("-b", "--begin", dest="begintime", help="begin time, yyyy-mm-dd hh:mm:ss", metavar="DATETIME")
parser.add_option("-e", "--end", dest="endtime", help="end time, yyyy-mm-dd hh:mm:ss", metavar="DATETIME")
parser.add_option("-p", "--ip", dest="statisip", help="statis ip, xxx.xxx.xxx.xxx", metavar="STRING")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
begintime = options.begintime
endtime = options.endtime
statis_ip = options.statisip
verbosemode = options.verbosemode

if begintime==None:
  logger.error("BeginTime is missing.")
  sys.exit(-101)

if isdatetime(begintime)==False:
  logger.error("unconverted datetime %s" % begintime)
  sys.exit(-102)
dt_begintime = datetime.datetime.strptime(begintime,"%Y-%m-%d %H:%M:%S")
ts_begintime = long(dt_begintime.strftime("%s"))

if endtime==None:
  logger.error("EndTime is missing.")
  sys.exit(-103)

if isdatetime(endtime)==False:
  logger.error("unconverted datetime %s" % endtime)
  sys.exit(-104)
dt_endtime = datetime.datetime.strptime(endtime,"%Y-%m-%d %H:%M:%S")
ts_endtime = long(dt_endtime.strftime("%s"))

if statis_ip==None:
  logger.error("Statis IP is missing.")
  sys.exit(-107)

if isipv4(statis_ip)==False:
  logger.error("unconverted ip %s" % statis_ip)
  sys.exit(-108)

if ts_begintime>=ts_endtime:
  begintime, endtime = endtime, begintime
  dt_begintime, dt_endtime = dt_endtime, dt_begintime
  ts_begintime, ts_endtime = ts_endtime, ts_begintime

if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

#nginx日志文件正则解析规则
r_remoteAddr = r"?P<remoteAddr>[\d.]*"
r_host = r"""?P<host>[-a-zA-Z0-9.]*"""
#r_logTime = r"""?P<logTime>[\w:/]+\s[+\-]\d{4}"""   #此为含时区的格式
r_logTime = r"?P<logTime>\S+"
r_timeZone = r"?P<timeZone>\S+"
r_method = r"?P<method>\S+"
r_request = r"?P<request>\S+"
r_protocol = r"?P<protocol>\S+"
r_status = r"?P<status>\S+"
r_bodyBytesSent = r"?P<bodyBytesSent>\d+"
r_referer = r"""?P<referer>[^\"]*"""
r_userid = r"""?P<userid>[-0-9]*"""
r_requestDur = r"""?P<requestDur>[-0-9.]*"""
r_session = r"""?P<session>[-a-z0-9A-Z]*"""
r_httpXRequestedWith = r"""?P<httpXRequestedWith>[^\"]*"""
r_httpUserAgent = r"""?P<userAgent>[^\"]*"""
r_upstreamResponseDur = r"""?P<upstreamResponseDur>[^\"]*"""
logformat = re.compile(r"(%s)\ - (%s)\ \[(%s) (%s)\]\ \"(%s)?[\s]?(%s)?[\s]?(%s)?.*?\"\ (%s)\ (%s)\ \"(%s)\"\ (%s)\ (%s)\ (%s)\ \"(%s)\"\ \"(%s)\"\ \"(%s).*?\"" % \
  (r_remoteAddr, r_host, r_logTime, r_timeZone, r_method, r_request, r_protocol, r_status, r_bodyBytesSent, r_referer, r_userid, r_requestDur, r_session, r_httpXRequestedWith, r_httpUserAgent, r_upstreamResponseDur) )

outfieldnum=16
c_remoteAddr   , \
c_host   , \
c_logTime   , \
c_timeZone   , \
c_method   , \
c_request   , \
c_protocol   , \
c_status   , \
c_bodyBytesSent    , \
c_referer    , \
c_userid    , \
c_requestDur    , \
c_session    , \
c_httpXRequestedWith    , \
c_httpUserAgent    , \
c_upstreamResponseDur = range(outfieldnum)

#输出格式。按照applog文件格式
fieldsdelimiter, rowsdelimiter = "\t", "\n"
resultrowformat = \
"%(request_time)d" + fieldsdelimiter + \
"%(device_id)s" + fieldsdelimiter + \
"%(channel_id)s" + fieldsdelimiter + \
"%(phone_type)s" + fieldsdelimiter + \
"%(userip)s" + fieldsdelimiter + \
"%(appid)d" + fieldsdelimiter + \
"%(version_id)s" + fieldsdelimiter + \
"%(userid)s" + fieldsdelimiter + \
"%(function_id)s" + fieldsdelimiter + \
"%(parameter_desc)s" + fieldsdelimiter + \
"%(uuid)s" + rowsdelimiter
resultset, resultchunk, chunksize = {}, [], 200000

#按小时循环从nginx日志中抽取数据
dt_statistime = dt_begintime - datetime.timedelta(minutes=dt_begintime.minute,seconds=dt_begintime.second)
while dt_statistime<=dt_endtime:
  statis_date = dt_statistime.strftime("%Y-%m-%d")
  statis_hour = dt_statistime.strftime("%H")
  #源文件
  nginxfile = "/backup/nginx/%(statis_date)s/nginx-info-%(statis_hour)s-%(statis_ip)s.log.lzo" % {'statis_date':statis_date, 'statis_hour':statis_hour, 'statis_ip':statis_ip}
  #结果临时文件和解析错误记录文件
  tmpfilename = 'applog_%(statis_date)s-%(statis_hour)s-%(statis_ip)s_nginx.log' % {'statis_date':statis_date, 'statis_hour':statis_hour, 'statis_ip':statis_ip}
  tmpfile = '%(dir)s/%(tmpfilename)s' % {'dir':tmpdir, 'tmpfilename':tmpfilename}
  tmpwriter = open(tmpfile,'w+')
  errfilename = 'applog_%(statis_date)s-%(statis_hour)s-%(statis_ip)s_nginx.err' % {'statis_date':statis_date, 'statis_hour':statis_hour, 'statis_ip':statis_ip}
  errfile = '%(dir)s/%(errfilename)s' % {'dir':tmpdir, 'errfilename':errfilename}
  errwriter = open(errfile,'w+')
  #读取源文件
  cmd = "hdfs dfs -text %(hdfsfile)s" % {'hdfsfile':nginxfile}
  logger.debug("cmd: %s" % cmd)
  hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  rownum, errnum, voidnum = 0, 0, 0
  for logline in hdfs.stdout:
    rownum += 1
    if rownum%chunksize==0:
      logger.debug("row> %d" % rownum)
      #break #测试时打开。只取部分数据处理，以便快速进入下一步
    matchrow = logformat.match(logline)
    #如果当前日志行在格式匹配错误，写入错误文件，跳到下一行
    if matchrow==None:
      errnum += 1
      errwriter.write(logline)
      continue
    outrow = matchrow.groups()
    #utctime = datetime.datetime.strptime(outrow[c_logTime],'%d/%b/%Y:%H:%M:%S') - datetime.timedelta(hours=int(outrow[c_timeZone][:3]), minutes=int(outrow[c_timeZone][0]+outrow[c_timeZone][3:]))  
    cur_request_time = long(datetime.datetime.strptime(outrow[c_logTime],'%d/%b/%Y:%H:%M:%S').strftime("%s"))
    #如果不在抽取时间范围内，忽略，跳到下一行
    if cur_request_time<ts_begintime or cur_request_time>ts_endtime:
      voidnum += 1
      continue
    cur_host = outrow[c_host]
    cur_request = urlparse.urlparse(outrow[c_request])
    cur_path = cur_request.path
    #如果当前日志URL不为预期API地址，忽略，跳到下一行
    if cur_host=='api.haodou.com' and cur_path=='/index.php':  #http://api.haodou.com/index.php
      pass
    elif cur_host=='api.hoto.cn' and cur_path=='/index.php':   #http://api.hoto.cn/index.php
      pass
    elif cur_host=='www.haodou.com' and cur_path=='/api.php':  #http://www.haodou.com/api.php
      pass
    else:
      voidnum += 1
      continue
    cur_params = urlparse.parse_qs(cur_request.query)
    if cur_params.get('deviceid')==None:
      voidnum += 1
      continue
    cur_device_id = cur_params['deviceid'][0]
    cur_channel_id = '' if cur_params.get('channel')==None else cur_params['channel'][0]
    cur_phone_type = ''
    cur_userip = outrow[c_remoteAddr]
    if cur_params.get('appid')==None:
      voidnum += 1
      continue
    cur_appid = int(cur_params['appid'][0])
    cur_version_id = '' if cur_params.get('vn')==None else cur_params['vn'][0]
    cur_userid = '0'
    if cur_params.get('method')==None:
      voidnum += 1
      continue
    cur_function_id = cur_params['method'][0].lower()
    cur_parameter_desc = '{}'
    cur_uuid = '' if cur_params.get('uuid')==None else cur_params['uuid'][0]
    resultline = resultrowformat % { \
      'request_time':cur_request_time, \
      'device_id':cur_device_id, \
      'channel_id':cur_channel_id, \
      'phone_type':cur_phone_type, \
      'userip':cur_userip, \
      'appid':cur_appid, \
      'version_id':cur_version_id, \
      'userid':cur_userid, \
      'function_id':cur_function_id, \
      'parameter_desc':cur_parameter_desc, \
      'uuid':cur_uuid }
    resultchunk.append(resultline)
    if rownum%chunksize==0:
      try:
        tmpwriter.writelines(resultchunk)
        resultchunk = []
      except:
        tmpwriter.close()
  errwriter.close()
  tmpwriter.writelines(resultchunk)
  tmpwriter.close()
  logger.debug("statistics info (nginx log)\n rownum: %d\n errnum: %d\n voidnum: %d" % (rownum,errnum,voidnum))
  #进入下一个小时
  dt_statistime = dt_statistime + datetime.timedelta(hours=1)

print "end."
sys.exit(0)
