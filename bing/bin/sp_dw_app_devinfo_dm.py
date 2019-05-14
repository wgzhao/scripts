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
logger = logging.getLogger("devinfo")
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
tmpfilename = 'devinfo_%(statisdate)s.dat' % {'statisdate':statisdate}
tmpfile = '%(dir)s/%(filename)s' % {'dir':tmpdir, 'filename':tmpfilename}
tmpwriter = open(tmpfile,'w+')

##日期变量
dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
statis_date = dt_statisdate.strftime("%Y-%m-%d")
statisdate = dt_statisdate.strftime("%Y%m%d")
curdate = rundate
cur_date = "%s-%s-%s" % (curdate[0:4],curdate[4:6],curdate[6:8])

logger.debug("reading requestlog (%s)..." % statis_date)

cmd = "hdfs dfs -text /bing/data/ods_app_requestlog_dm/statis_date=%(statis_date)s/source_type=applog/*.lzo" % {'statis_date':statis_date}
logger.debug("cmd: %s" % cmd)
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

outfieldnum=11
c_request_time   , \
c_device_id      , \
c_channel_id     , \
c_userip         , \
c_app_id         , \
c_version_id     , \
c_userid         , \
c_function_id    , \
c_parameter_info , \
c_parameter_desc , \
c_uuid = range(outfieldnum)

fieldsdelimiter, rowsdelimiter = "\t", "\n"
resultrowformat = \
"%(app_id)s" + fieldsdelimiter + \
"%(device_id)s" + fieldsdelimiter + \
"%(dev_uuid)s" + fieldsdelimiter + \
"%(dev_imei)s" + fieldsdelimiter + \
"%(dev_mac)s" + fieldsdelimiter + \
"%(dev_idfa)s" + fieldsdelimiter + \
"%(dev_idfv)s" + fieldsdelimiter + \
"%(dev_brand)s" + fieldsdelimiter + \
"%(dev_model)s" + fieldsdelimiter + \
"%(dev_os)s" + fieldsdelimiter + \
"%(dev_osver)s" + fieldsdelimiter + \
"%(scr_width)s" + fieldsdelimiter + \
"%(scr_height)s" + fieldsdelimiter + \
"%(operator_name)s" + fieldsdelimiter + \
"%(operator_code)s" + fieldsdelimiter + \
"%(dev_rooted)d" + rowsdelimiter
resultset, resultchunk = {}, []

rownum, errnum, devnum = 0, 0, 0
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
    #break #测试时打开。只取部分数据处理，以便快速进入下一步
  outrow = outline.strip('\n').split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  if outrow[c_function_id] not in ['ad.getxfad']:
    errnum += 1
    continue
  if outrow[c_app_id] not in ['2','4']:
    errnum += 1
    continue
  curkey = '$' + str(outrow[c_app_id]) + '$' + str(outrow[c_device_id])
  if curkey not in resultset:
    resultset[curkey] = 1
    devnum += 1
    cur_app_id = outrow[c_app_id]
    cur_device_id = outrow[c_device_id]
    cur_dev_uuid = outrow[c_uuid]
    try:
      parainfo = ujson.loads(outrow[c_parameter_info])
      devinfo = ujson.loads(str(parainfo.get('deviceinfo')))
    except ValueError:
      errnum += 1
      continue
    if devinfo is None:
      errnum += 1
      continue
    cur_dev_imei = devinfo.get('imei') if devinfo.get('imei') is not None else ''
    cur_dev_mac = devinfo.get('mac') if devinfo.get('mac') is not None else ''
    cur_dev_idfa = devinfo.get('idfa') if devinfo.get('idfa') is not None else ''
    cur_dev_idfv = devinfo.get('idfv') if devinfo.get('idfv') is not None else ''
    cur_dev_brand = devinfo.get('brand') if devinfo.get('brand') is not None else 'Apple' if cur_app_id=='4' else 'Unknown'
    cur_dev_model = devinfo.get('model') if devinfo.get('model') is not None else 'Unknown'
    cur_dev_os = 'Android' if cur_app_id=='2' else 'iOS' if cur_app_id=='4' else 'Unknown'
    cur_dev_osver = devinfo.get('os_version_name')
    cur_scr_width = devinfo.get('width')
    cur_scr_height = devinfo.get('height')
    cur_operator_name = devinfo.get('operator_name') if devinfo.get('operator_name') is not None else ''
    cur_operator_code = devinfo.get('operator_code') if devinfo.get('operator_code') is not None else ''
    cur_dev_rooted = int(devinfo.get('root'))
    resultline = resultrowformat % { \
      'app_id':cur_app_id, \
      'device_id':cur_device_id, \
      'dev_uuid':cur_dev_uuid, \
      'dev_imei':cur_dev_imei, \
      'dev_mac':cur_dev_mac, \
      'dev_idfa':cur_dev_idfa, \
      'dev_idfv':cur_dev_idfv, \
      'dev_brand':cur_dev_brand, \
      'dev_model':cur_dev_model, \
      'dev_os':cur_dev_os, \
      'dev_osver':cur_dev_osver, \
      'scr_width':cur_scr_width, \
      'scr_height':cur_scr_height, \
      'operator_name':cur_operator_name, \
      'operator_code':cur_operator_code, \
      'dev_rooted':cur_dev_rooted }
    resultchunk.append(resultline)
hdfs.terminate()
logger.debug("statistics info (app_requestlog.log)\n rownum: %d\n errnum: %d" % (rownum,errnum))
logger.debug(" device number: %d" % (devnum))

tmpwriter.writelines(resultchunk)
tmpwriter.close()

logger.debug("zipping file ...")
if os.path.exists(tmpfile+".lzo"):
  runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})
runShell("cd %(tmpdir)s && lzop %(tmpfilename)s" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})

logger.debug("loading data ...")
runHiveQL("load data local inpath '%(tmpfile)s.lzo' overwrite into table bing.dw_app_devinfo_dm partition (statis_date='%(statis_date)s');" % {'tmpfile':tmpfile, 'statis_date':statis_date})

logger.debug("deleting tmpfile ...")
runShell("cd %(tmpdir)s && ls -l %(tmpfilename)s* | grep -v .lst > %(tmpfilename)s.lst" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
runShell("rm %(tmpfile)s" % {'tmpfile':tmpfile})
runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)
