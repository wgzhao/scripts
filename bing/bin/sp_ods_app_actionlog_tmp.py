#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 性能说明：python2.6带的json库，运行220w条，要4个半小时，改为ujson库，只要20多分钟

import sys, os, datetime, time
import string, codecs, ujson
import logging
from optparse import OptionParser
import subprocess
import math

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

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

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
logger = logging.getLogger("actionlog")
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
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,"%Y%m%d")+datetime.timedelta(days=-1)).strftime("%Y%m%d"))
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
statisdate = options.statisdate.replace('-','')
verbosemode = options.verbosemode

if isdate(statisdate)==False:
  logger.error("unconverted date %s" % statisdate)
  sys.exit(-101)

if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

##先确保临时文件能操作，如果临时文件不能操作，则后继处理无意义
actionfilename = 'actionlog_%(statisdate)s_tmp.log' % {'statisdate':statisdate}
actionfile = '%(dir)s/%(filename)s' % {'dir':tmpdir, 'filename':actionfilename}
actionwriter = open(actionfile,'w+')

##日期变量
dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
statis_date = dt_statisdate.strftime("%Y-%m-%d")
statisdate = dt_statisdate.strftime("%Y%m%d")
curdate = rundate
cur_date = "%s-%s-%s" % (curdate[0:4],curdate[4:6],curdate[6:8])

logger.debug("reading actionlog (%s)..." % statis_date)

cmd = "hdfs dfs -text /backup/behaviour/app_action/%(statis_date)s/*.lzo" % {'statis_date':statis_date}
logger.debug("cmd: %s" % cmd)
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

applist = ['2','4','6']
actionact, ignorepage, actioncnt = ['A4000','A4001','A4002','A4003','A4004','A4005','A4006'], [], 0

fieldsdelimiter, rowsdelimiter = "\t", "\n"
resultrowformat = \
"%(access_timestamp)d" + fieldsdelimiter + \
"%(access_time)s" + fieldsdelimiter + \
"%(dev_uuid)s" + fieldsdelimiter + \
"%(app_id)s" + fieldsdelimiter + \
"%(channel_id)s" + fieldsdelimiter + \
"%(version_id)s" + fieldsdelimiter + \
"%(userip)s" + fieldsdelimiter + \
"%(userid)s" + fieldsdelimiter + \
"%(action)s" + fieldsdelimiter + \
"%(page)s" + fieldsdelimiter + \
"%(access_info)s" + rowsdelimiter
chunksize, resultchunk = 200000, []

rownum, errnum = 0, 0
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
    #break #测试代码。只取部分数据处理，以便快速进入下一步
  outline = outline.strip()
  if outline[0]!='{' or outline[-1]!='}':
    errnum += 1
    continue
  try:
    info = ujson.loads(outline)
  except ValueError:
    errnum += 1
    continue
  if info.get('b') is None:
    errnum += 1
    continue
  if info.get('ext') is None:
    errnum += 1
    continue
  row_dev_uuid   = info['b'].get('a')
  row_channel_id = info['b'].get('channel')
  #老版本appid与versionid存放的key错误，需要人为调整
  row_app_id     = info['b'].get('d') if info['b'].get('d') in applist else info['b'].get('e')
  row_version_id = info['b'].get('e') if info['b'].get('d') in applist else info['b'].get('d')
  if row_dev_uuid is None or row_channel_id is None or row_app_id is None or row_version_id is None:
    errnum += 1
    continue
  if row_app_id!='2':
    continue
  row_time   = info['b'].get('time')
  if row_time is not None:
    row_time = datetime.datetime.fromtimestamp(int(row_time)).strftime('%Y-%m-%d %H:%M:%S')
  row_userip = info.get('user_ip')
  extlist    = []
  if isinstance(info['ext'], list):
    extlist = info['ext']
  if isinstance(info['ext'], dict):
    extlist.append(info['ext'])
  cur_page = ''
  for extinfo in extlist:
    if extinfo is None:
      continue
    cur_action = extinfo.get('action')
    if cur_action in actionact:
      page = extinfo.get('page')
      if page in ignorepage:
        continue
      if cur_page!=page:
        actioncnt += 1
        cur_page   = page
        cur_time   = extinfo.get('time').split('.')[0] if extinfo.get('time') else row_time
        cur_timestamp = time.mktime(time.strptime(cur_time,'%Y-%m-%d %H:%M:%S'))
        cur_userip = extinfo.get('ip') if extinfo.get('ip') else row_userip
        resultline = resultrowformat % { \
          'access_timestamp':cur_timestamp, \
          'access_time':cur_time, \
          'dev_uuid':row_dev_uuid, \
          'app_id':row_app_id, \
          'channel_id':row_channel_id, \
          'version_id':row_version_id, \
          'userip':cur_userip, \
          'userid':'', \
          'action':cur_action, \
          'page':cur_page, \
          'access_info':'' }  #ujson.dumps(extinfo)
        resultchunk.append(resultline)
        if actioncnt % chunksize == 0:
          try:
            actionwriter.writelines(resultchunk)
            resultchunk = []
          except:
            actionwriter.close()
hdfs.terminate()
logger.debug("statistics info (app_action.log)\n rownum: %d\n errnum: %d" % (rownum,errnum))
logger.debug(" action count: %d" % (actioncnt))

actionwriter.writelines(resultchunk)
actionwriter.close()

logger.info("end.(%d)" % retval)
sys.exit(retval)
