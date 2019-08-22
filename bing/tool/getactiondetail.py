#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
parser.add_option("-a", "--action", dest="actioncode", help="action code. AAAAA[,BBBBB[,...]]", metavar="STRING")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
statisdate = options.statisdate.replace('-','')
actioncode = options.actioncode
verbosemode = options.verbosemode

if isdate(statisdate)==False:
  logger.error("unconverted date %s" % statisdate)
  sys.exit(-101)

if actioncode==None:
  logger.error("Action code is missing.")
  sys.exit(-102)

if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

##先确保临时文件能操作，如果临时文件不能操作，则后继处理无意义
tmpfilename = 'actionlog_%(action)s_%(statisdate)s.log' % {'action':actioncode.replace(',','_'),'statisdate':statisdate}
tmpfile = '%(dir)s/%(filename)s' % {'dir':tmpdir, 'filename':tmpfilename}
tmpwriter = open(tmpfile,'w+')

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

actionlist, actioncnt = actioncode.split(','), 0

fieldsdelimiter, rowsdelimiter = "\t", "\n"
resultrowformat = \
"%(access_time)s" + fieldsdelimiter + \
"%(dev_uuid)s" + fieldsdelimiter + \
"%(app_id)s" + fieldsdelimiter + \
"%(channel_id)s" + fieldsdelimiter + \
"%(version_id)s" + fieldsdelimiter + \
"%(userip)s" + fieldsdelimiter + \
"%(userid)s" + fieldsdelimiter + \
"%(action_info)s" + rowsdelimiter
chunksize, resultchunk = 200000, []

rownum, errnum = 0, 0
for outline in hdfs.stdout:
  rownum += 1
  #if rownum>=2000:
  #  break #测试代码。只取部分数据处理，以便快速进入下一步
  if (rownum%chunksize)==0:
    logger.debug("row> %d" % rownum)
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
  row_app_id     = info['b'].get('d') if info['b'].get('d') in ['2','4'] else info['b'].get('e')
  row_version_id = info['b'].get('e') if info['b'].get('d') in ['2','4'] else info['b'].get('d')
  if row_dev_uuid is None or row_channel_id is None or row_app_id is None or row_version_id is None:
    errnum += 1
    continue
  if row_app_id not in ['2','4']:
    continue
  row_time   = info['b'].get('time')
  if row_time is not None:
    row_time = datetime.datetime.fromtimestamp(int(row_time)).strftime('%Y-%m-%d %H:%M:%S')
  row_userip = info.get('user_ip')
  extlist    = []
  if isinstance(info['ext'], list):
    extlist = info['ext']
  elif isinstance(info['ext'], dict):
    extlist.append(info['ext'])
  for extinfo in extlist:
    if extinfo is None:
      continue
    cur_action = extinfo.get('action')
    if cur_action in actionlist:
      actioncnt += 1
      cur_time   = extinfo.get('time') if extinfo.get('time') else row_time
      cur_userip = extinfo.get('ip') if extinfo.get('ip') else row_userip
      resultline = resultrowformat % { \
        'access_time':cur_time, \
        'dev_uuid':row_dev_uuid, \
        'app_id':row_app_id, \
        'channel_id':row_channel_id, \
        'version_id':row_version_id, \
        'userip':cur_userip, \
        'userid':'', \
        'action_info':ujson.dumps(extinfo) }
      resultchunk.append(resultline)
      if actioncnt % chunksize == 0:
        try:
          tmpwriter.writelines(resultchunk)
          resultchunk = []
        except:
          tmpwriter.close()
hdfs.terminate()
logger.debug("statistics info (app_action.log)\n rownum: %d\n errnum: %d" % (rownum,errnum))
logger.debug(" action count: %d" % (actioncnt))

tmpwriter.writelines(resultchunk)
tmpwriter.close()

#logger.debug("zipping file ...")
#if os.path.exists(tmpfile+".lzo"):
#  runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})
#runShell("cd %(tmpdir)s && lzop %(tmpfilename)s" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
#
#logger.debug("loading data ...")
#runHiveQL("load data local inpath '%(tmpfile)s.lzo' overwrite into table bing. partition (statis_date='%(statis_date)s');" % {'tmpfile':tmpfile, 'statis_date':statis_date})
#
#logger.debug("deleting tmpfile ...")
#runShell("cd %(tmpdir)s && ls -l %(tmpfilename)s* | grep -v .lst > %(tmpfilename)s.lst" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
#runShell("rm %(tmpfile)s" % {'tmpfile':tmpfile})
#runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)