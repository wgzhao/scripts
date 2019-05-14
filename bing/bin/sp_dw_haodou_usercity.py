#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import logging
from optparse import OptionParser
import string, codecs
import subprocess
import json
import re
#haodou
from lib.geoip2pack import GeoIP2Reader

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

def rowformat(tablefields, fieldsdelimiter='\t', rowsdelimiter='\n'):
  fields = tablefields.split(',')
  ret, idx = '', 0
  while idx<len(fields):
    ret += '%%(%s)s' % fields[idx].strip()
    if idx<len(fields)-1:
      ret += fieldsdelimiter
    else:
      ret += rowsdelimiter
    idx += 1
  return ret

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
usageinfo = "%prog [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.1.0")
parser.add_option('-v', '--verbose', action='store_true', dest='verbosemode', default=False, help='verbose mode', metavar='MODE')

(options, args) = parser.parse_args()
verbosemode = options.verbosemode

#参数检查
if verbosemode:
  consoleHandler.setLevel(logging.DEBUG)

#城市维表数据加载 从hive库bing.dw_haodou_city_geoip表
logger.debug("loading city data ...")
outfieldnum = 6
c_city, c_province, c_country, c_city_cn, c_province_cn, c_country_cn = range(outfieldnum)

cmd = 'hdfs dfs -text /bing/ext/dw_haodou_city_geoip/city.dat'
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

cityset = {}
rownum, errnum = 0, 0
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
  outrow = outline.strip('\n').split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  cur_key = '$' + outrow[c_city] + '$' + outrow[c_province] + '$' + outrow[c_country]
  if cur_key not in cityset:
    cityset[cur_key] = {}
    cityset[cur_key]['city'] = outrow[c_city]
    cityset[cur_key]['province'] = outrow[c_province]
    cityset[cur_key]['country'] = outrow[c_country]
    cityset[cur_key]['city_cn'] = outrow[c_city_cn]
    cityset[cur_key]['province_cn'] = outrow[c_province_cn]
    cityset[cur_key]['country_cn'] = outrow[c_country_cn]
hdfs.terminate()
logger.debug("statistics info (bing.dw_haodou_city_geoip)\n rownum: %d\n errnum: %d" % (rownum,errnum))

#用户资料数据加载 从hive库haodou_passport_yyyymmdd.User表
logger.debug("loading user data ...")

outfieldnum = 18
c_userid      , \
c_email       , \
c_mobile      , \
c_username    , \
c_modifyname  , \
c_password    , \
c_salt        , \
c_pwdstrong   , \
c_regtime     , \
c_regip       , \
c_logintime   , \
c_loginip     , \
c_status      , \
c_wealth      , \
c_fromplatform, \
c_fromproduct , \
c_fromway     , \
c_emailact    = range(outfieldnum)

cmd = 'hdfs dfs -text /apps/hive/warehouse/haodou_warehouse/haodou_passport_%(rundate)s/user/*' % {'rundate':rundate}
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

reader = GeoIP2Reader()
resultset = {}
rownum, errnum, errnum_invalidip, errnum_notfound = 0, 0, 0, 0
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
  outrow = outline.strip('\n').split("\001")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  cur_userid    = int(outrow[c_userid])
  cur_username  = outrow[c_username]
  cur_loginip   = outrow[c_loginip]
  cur_logintime = outrow[c_logintime]
  if not isipaddrv4(cur_loginip):
    errnum_invalidip += 1
    errnum += 1
    continue
  resultset[cur_userid] = {}
  resultset[cur_userid]['userid'] = cur_userid
  resultset[cur_userid]['username'] = cur_username
  resultset[cur_userid]['loginip'] = cur_loginip
  resultset[cur_userid]['logintime'] = cur_logintime
  ipinfo = reader.ip2city(cur_loginip)
  resultset[cur_userid]['city'] = ipinfo['city']
  resultset[cur_userid]['province'] = ipinfo['province']
  resultset[cur_userid]['country'] = ipinfo['country']
  citykey = '$' + ipinfo['city'] + '$' + ipinfo['province'] + '$' + ipinfo['country']
  if citykey not in cityset:
    cityset[citykey] = {}
    cityset[citykey]['city'] = ipinfo['city']
    cityset[citykey]['province'] = ipinfo['province']
    cityset[citykey]['country'] = ipinfo['country']
    cityset[citykey]['city_cn'] = ipinfo['city_cn']
    cityset[citykey]['province_cn'] = ipinfo['province_cn']
    cityset[citykey]['country_cn'] = ipinfo['country_cn']
hdfs.terminate()
reader.close()
logger.debug("statistics info (user)\n rownum: %d\n errnum: %d" % (rownum,errnum))
logger.debug(" Invalid IP: %d\n Not Found: %d" % (errnum_invalidip,errnum_notfound))

#输出结果集格式定义（与结果表bing.dw_haodou_cityuser 结构一致）
resultfield = 'userid, username, city, province, country, loginip, logintime'
resultrowformat = rowformat(resultfield)

#输出结果集文件 
tmpfilename = "usercity.dat" #未分区，只有一个文件
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
logger.debug("writing file (%(file)s)..." % {'file':tmpfilename})
tmpwriter = open(tmpfile,'w')
pos, chunksize, resultchunk = 0, 200000, []
for item in resultset:
  pos += 1
  cur_userid    = resultset[item]['userid']
  cur_username  = resultset[item]['username']
  cur_city      = resultset[item]['city']
  cur_province  = resultset[item]['province']
  cur_country   = resultset[item]['country']
  cur_loginip   = resultset[item]['loginip']
  cur_logintime = resultset[item]['logintime']
  resultline = resultrowformat % { \
    'userid':cur_userid, \
    'username':cur_username, \
    'city':cur_city, \
    'province':cur_province, \
    'country':cur_country, \
    'loginip':cur_loginip, \
    'logintime':cur_logintime \
    }
  resultchunk.append(resultline)
  if pos % chunksize == 0:
    try:
      tmpwriter.writelines(resultchunk)
      resultchunk = []
    except:
      tmpwriter.close()
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#LZO压缩数据文件 与表存储格式保持一致 stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
logger.debug("zipping file ...")
zipfile = tmpfile+".lzo"
if os.path.exists(zipfile):
  runShell("rm %s" % zipfile)
runShell("cd %(tmpdir)s && lzop %(tmpfilename)s" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
#加载到hdfs的外部表目录下 该目录由结果表bing.dw_haodou_usercity 建表语句的location指定
logger.debug("loading hdfs usercity ...")
runShell("hdfs dfs -moveFromLocal -f %(file)s /bing/ext/dw_haodou_usercity" % {'file':zipfile})

#城市维表格式定义（与结果表bing.dw_haodou_city_geoip 结构一致）
cityfield = 'city, province, country, city_cn, province_cn, country_cn'
cityrowformat = rowformat(cityfield)

#输出城市维表文件
tmpfilename = "city.dat" #未分区，只有一个文件
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
logger.debug("writing file (%(file)s)..." % {'file':tmpfilename})
tmpwriter = open(tmpfile,'w')
pos, chunksize, resultchunk = 0, 200000, []
for city in cityset:
  pos += 1
  cityline = cityrowformat % { \
    'city':cityset[city]['city'], \
    'province':cityset[city]['province'], \
    'country':cityset[city]['country'], \
    'city_cn':cityset[city]['city_cn'], \
    'province_cn':cityset[city]['province_cn'], \
    'country_cn':cityset[city]['country_cn'] \
    }
  resultchunk.append(cityline)
  if pos % chunksize == 0:
    try:
      tmpwriter.writelines(resultchunk)
      resultchunk = []
    except:
      tmpwriter.close()
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#加载到hdfs的外部表目录下 该目录由结果表bing.dw_haodou_city_geoip 建表语句的location指定
logger.debug("loading hdfs city ...")
runShell("hdfs dfs -moveFromLocal -f %(file)s /bing/ext/dw_haodou_city_geoip" % {'file':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)
