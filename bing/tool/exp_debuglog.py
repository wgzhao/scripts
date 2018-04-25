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

def mkfile(s, m):
  if not os.path.exists(s):
    f=open(s,'w')
    f.close()
    os.chmod(s, m)

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

def sqlescape(s):
  try:
    ret = str(s).replace('\\','\\\\')
    ret = ret.replace('`','\\`')
    return ret
  except:
    return None

def list2str(l):
  return "'" + "','".join(l) + "'"

def runHiveQL(sql):
  cmd = 'hive -S -e "%(sql)s"' % {'sql':sqlescape(sql)}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  hiveout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("HiveError!!!(%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return hiveout

def extractApplog(feedid, deviceid, debugdate):
  sqlstmt = R"""
  create table if not exists bing.tmp_applog_debug
  (
    request_time      timestamp,
    device_id         string,
    userip            string,
    app_id            string,
    userid            string,
    uuid              string,
    function_id       string,
    request_id        string,
    req_info          string
  ) partitioned by (feedid string)
    row format delimited fields terminated by '\t' stored as textfile
  ;

  insert overwrite table bing.tmp_applog_debug partition (feedid='${feedid}')
  select request_time, device_id, userip, app_id, userid, uuid, function_id, 
  get_json_object(parameter_info,'$.request_id'), parameter_info
  from bing.ods_app_requestlog_dm
  where statis_date='${debugdate}' and uuid='${deviceid}'
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${feedid}',feedid)
  sql = sql.replace('${deviceid}',deviceid)
  sql = sql.replace('${debugdate}',debugdate)
  logger.debug("sql = %s" % sql)
  runHiveQL(sql)

def existsAppResponselogTmp(debugdate):
  sqlstmt = R"""
  create table if not exists bing.tmp_app_responselog_dm
  (
    request_id string,
    status string,
    response_time timestamp,
    resp_info  string
  ) partitioned by (statis_date string)
    row format delimited fields terminated by '\t'
    stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
    outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  ;

  set hive.cli.print.header=false;
  
  select t.* 
  from bing.tmp_app_responselog_dm t
  where statis_date='${debugdate}' 
  limit 100
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${debugdate}',debugdate)
  logger.debug("sql = %s" % sql)
  hiveout = runHiveQL(sql)
  if len(hiveout.splitlines())==100:
    return True
  else:
    return False

def extractAppResponselog(debugdate):
  sqlstmt = R"""
  create table if not exists bing.tmp_app_responselog_dm
  (
    request_id string,
    status string,
    response_time timestamp,
    resp_info  string
  ) partitioned by (statis_date string)
    row format delimited fields terminated by '\t'
    stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
    outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  ;

  set mapreduce.map.memory.mb=8192;
  set mapreduce.reduce.memory.mb=8192;
  set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
  set mapreduce.output.fileoutputformat.compress=true;
  set hive.exec.compress.output=true;
  
  insert overwrite table bing.tmp_app_responselog_dm partition (statis_date='${debugdate}')
  select get_json_object(json,'$.request_id') as request_id, 
  get_json_object(json,'$.status') as status,
  from_unixtime(cast(get_json_object(json,'$.time') as bigint)) as response_time,
  json as resp_info
  from logs.log_php_app_resp_log
  where logdate='${debugdate}'
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${debugdate}',debugdate)
  logger.debug("sql = %s" % sql)
  runHiveQL(sql)

def exportDebugResponselog(feedid, debugdate, functions, outputfile):
  sqlstmt = R"""
  set hive.cli.print.header=true;
  set mapreduce.map.memory.mb=8192;
  set mapreduce.reduce.memory.mb=8192;
  set hive.auto.convert.join=false;

  select /*+ mapjoin(d)*/
  d.*, l.status, l.response_time, l.resp_info
  from
  (select * 
  from bing.tmp_applog_debug
  where feedid='${feedid}'
  and function_id in (${functions})
  ) d
  left outer join
  (select *
  from bing.tmp_app_responselog_dm
  where statis_date='${debugdate}'
  ) l on (d.request_id=l.request_id)
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${feedid}',feedid)
  sql = sql.replace('${debugdate}',debugdate)
  sql = sql.replace('${functions}',functions)
  logger.debug("sql = %s" % sql)
  hiveout = runHiveQL(sql)
  fp = open(outputfile, "w")
  try:
    fp.writelines(hiveout)
  finally:
    fp.close()

def exportDebugCrashlog(deviceid, debugdate, outputfile):
  sqlstmt = R"""
  set hive.cli.print.header=true;

  select l.*
  from bing.ods_app_actionlog_raw_dm l
  where statis_date='${debugdate}'
  and get_json_object(json_msg,'$.b.a')='${deviceid}'
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${deviceid}',deviceid)
  sql = sql.replace('${debugdate}',debugdate)
  logger.debug("sql = %s" % sql)
  hiveout = runHiveQL(sql)
  fp = open(outputfile, "w")
  try:
    fp.writelines(hiveout)
  finally:
    fp.close()

def exportDebugImagelog(deviceid, debugdate, outputfile):
  sqlstmt = R"""
  set hive.cli.print.header=true;
  
  select
  e.appid,
  e.channel,
  e.deviceid,
  e.sessionid,
  e.uuid,
  e.vc,
  e.vn,
  e.url,
  e.errormsg,
  e.requesttime,
  from_unixtime(bigint(e.starttime)) as starttime,
  e.errorcode,
  e.user_ip,
  from_unixtime(bigint(e.server_time)) as server_time,
  e.log_date
  from behavior.ods_app_err_log e 
  where log_date='${debugdate}' 
  and uuid='${deviceid}'
  ;
  """
  sql = delctrlchr(sqlstmt,'\t\r')
  sql = sql.replace('${deviceid}',deviceid)
  sql = sql.replace('${debugdate}',debugdate)
  logger.debug("sql = %s" % sql)
  hiveout = runHiveQL(sql)
  fp = open(outputfile, "w")
  try:
    fp.writelines(hiveout)
  finally:
    fp.close()

def todo():
  return true

SUPPORTED_TYPE='account/crash/image/publish/download/video/favorite/api'

reload(sys)
sys.setdefaultencoding('utf8')

retval = 0

##runtime variable
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]

##option parse
usageinfo = "sample: %prog --feed=231 --dev=5c337a77b8298361bf769e34751b775a --date=20150117 --type=account -v"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.0")
parser.add_option("-f", "--feed", dest="feedid", help="feedback sn", metavar="STRING")
parser.add_option("-d", "--dev", dest="deviceid", help="deviceid", metavar="STRING")
parser.add_option("-b", "--date", dest="debugdate", help="active date. yyyymmdd", metavar="DATE")
parser.add_option("-t", "--type", dest="debugtype", help="supported type: %s" % SUPPORTED_TYPE, metavar="STRING")
parser.add_option("-a", "--api", dest="function", help="function", metavar="STRING")
parser.add_option("-o", "--output", dest="outputfile", help="output file. default debuglog_<feedid>.txt", metavar="FILE")
parser.add_option("-z", "--zip", action="store_true", dest="zipmode", default=False, help="zip mode", metavar="MODE")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")
parser.add_option('-l', "--log", dest="logfile", default="exp_debuglog.log",help="log file to write")

(options, args) = parser.parse_args()
feedid = options.feedid
deviceid = options.deviceid
debugdate = options.debugdate
debugtype = options.debugtype
function = options.function
outputfile = options.outputfile
zipmode = options.zipmode
verbosemode = options.verbosemode


logfile = options.logfile # '%(dir)s%(sep)s%(filename)s.log' % {'dir':rundir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mkfile(logfile, 0666)

##logger
logger = logging.getLogger("debuglog")
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(logfile)
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(levelname)s - %(message)s"))
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(filename)s\n%(message)s"))
logger.addHandler(consoleHandler)

logger.info("begin ... %s" % str(sys.argv))


if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

if feedid==None:
  logger.error("feedback id is missing.")
  sys.exit(-101)
feedid = feedid.lower()

if deviceid==None:
  logger.error("deviceid is missing.")
  sys.exit(-102)

if debugdate==None:
  logger.error("debug date is missing.")
  sys.exit(-103)
debugdate = debugdate.replace('-','')

if isdate(debugdate)==False:
  logger.error("unconverted date %s" % debugdate)
  sys.exit(-104)

dt_debugdate = datetime.datetime.strptime(debugdate,"%Y%m%d")
debugdate = dt_debugdate.strftime("%Y-%m-%d")

if debugtype==None:
  logger.error("debug type is missing.")
  sys.exit(-105)
debugtype = debugtype.lower()

if debugtype not in SUPPORTED_TYPE.split('/'):
  logger.error("unsupported debug type. %s" % debugtype)
  sys.exit(-106)

if debugtype=='api' and function is None:
  logger.error("function is missing.")
  sys.exit(-107)

if debugtype=='api':
  function = function.lower()

if feedid.isdigit():
  debugid = "%s%s%s" % (feedid, function if debugtype=='api' else debugtype, dt_debugdate.strftime("%m%d"))
else:
  debugid = feedid

if outputfile==None:
  default_outputfilename = "debuglog_%(debugid)s" % {'debugid':debugid}
  default_ext = '.txt'
  outputfile = '%(dir)s%(sep)s%(filename)s%(ext)s' % {'dir':rundir,'sep':os.sep,'filename':default_outputfilename,'ext':default_ext}
else:
  outputfile = outputfile

if (os.path.exists(outputfile) and os.access(outputfile,os.W_OK)==False):
  logger.error("Output File is not writeable. %s" % outputfile)
  sys.exit(-104)

#handle feedback about account
if debugtype=='account':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = ['account.addconnect','account.bindconnectstatus','account.checkin','account.connectbindreg','account.getbindstatus',
    'account.getusersyncinfo','account.login','account.logout','account.reg','account.regbyphone','account.setaccesstoken','common.sendcode',
    'passport.bindconnectstatus','passport.connectbindreg','passport.login','passport.loginbyconnect','passport.logout','passport.reg']
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

#handle feedback about crash
if debugtype=='crash':
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugCrashlog(deviceid, debugdate, outputfile)

#handle feedback about image
if debugtype=='image':
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugImagelog(deviceid, debugdate, outputfile)

#handle feedback about publish
if debugtype=='publish':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = ['info.saveinfo','info.updateandpublic','info.uploadphoto','recipephoto.upload']
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

#handle feedback about download
if debugtype=='download':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = ['info.downloadinfo']
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

#handle feedback about video
if debugtype=='video':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = ['info.getvideourl']
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

#handle feedback about favorite
if debugtype=='favorite':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = ['favorite.add','favorite.addrecipe','favorite.getlist','favorite.getmyalbum','like.add']
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

#handle feedback about api
if debugtype=='api':
  logger.info("extract Applog (%s)..." % debugid)
  extractApplog(debugid, deviceid, debugdate)
  logger.info("extract Responselog (%s)..." % debugdate)
  if not existsAppResponselogTmp(debugdate):
    extractAppResponselog(debugdate)
  functionlist = function.split(',')
  logger.info("generate debuglog (%s)..." % outputfile)
  exportDebugResponselog(debugid, debugdate, list2str(functionlist), outputfile)

logger.info("end.(%d)" % retval)
sys.exit(retval)
