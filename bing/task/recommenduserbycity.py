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

resultset = {}
iptableset = {}
unknownstr = 'null'

#历史数据加载 从hive库bing.dw_haodou_cityuser表（外部表路径）获取
logger.debug("loading history data ...")

outfieldnum = 7
c_city, c_province, c_country, c_userid, c_lastip, c_lasttime, c_source = range(outfieldnum)

cmd = 'hdfs dfs -text /bing/ext/dw_haodou_cityuser/cityuser*.dat.lzo'
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

rownum, errnum = 0, 0
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
  outrow = outline.strip('\n').split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  cur_city = outrow[c_city]
  cur_province = outrow[c_province]
  cur_country = outrow[c_country]
  cur_userid = outrow[c_userid]
  cur_lastip = outrow[c_lastip]
  cur_lasttime = outrow[c_lasttime]
  cur_source = outrow[c_source]
  cur_key = '$' + cur_country + '$' + cur_province + '$' + cur_city
  if cur_key not in resultset:
    resultset[cur_key] = {}
    resultset[cur_key]['city'] = cur_city
    resultset[cur_key]['province'] = cur_province
    resultset[cur_key]['country'] = cur_country
    resultset[cur_key]['users'] = {}
    resultset[cur_key]['users'][cur_userid] = {}
    resultset[cur_key]['users'][cur_userid]['userid'] = cur_userid
    resultset[cur_key]['users'][cur_userid]['lastip'] = cur_lastip
    resultset[cur_key]['users'][cur_userid]['lasttime'] = cur_lasttime
    resultset[cur_key]['users'][cur_userid]['source'] = cur_source
  else:
    resultset[cur_key]['users'][cur_userid] = {}
    resultset[cur_key]['users'][cur_userid]['userid'] = cur_userid
    resultset[cur_key]['users'][cur_userid]['lastip'] = cur_lastip
    resultset[cur_key]['users'][cur_userid]['lasttime'] = cur_lasttime
    resultset[cur_key]['users'][cur_userid]['source'] = cur_source
  if cur_lastip not in iptableset:
    iptableset[cur_lastip] = {}
    iptableset[cur_lastip]['ip'] = cur_lastip
    iptableset[cur_lastip]['city'] = cur_city
    iptableset[cur_lastip]['province'] = cur_province
    iptableset[cur_lastip]['country'] = cur_country
hdfs.terminate()
logger.debug("statistics info (bing.dw_haodou_cityuser)\n rownum: %d\n errnum: %d" % (rownum,errnum))

#当天数据加载 从hive库logs.log_php_app_log表获取
logger.debug("loading today applog ...")

try:
  mmdbfile = '%(rundir)s/GeoLite2-City.mmdb' % {'rundir':rundir}
  ipreader = geoip2.database.Reader(mmdbfile)
except IOError:
  logger.error("Geoip2 IOError!%s" % mmdbfile)
  sys.exit(-1001)

outfieldnum = 11
c_requesttime, c_deviceid, c_channelid, c_phonetype, c_userip, c_appid, c_versionid, c_userid, c_functionid, c_parameterdesc, c_uuid = range(outfieldnum)

cmd = "hdfs dfs -text /backup/CDA39907/001/%(statis_date)s/CDA39907%(statisdate)s001.AVL.log.tar.lzo" % {'statis_date':statis_date, 'statisdate':statisdate}
logger.debug("cmd: %s" % cmd)
hdfs = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

rownum, errnum = 0, 0
errnum_nulluser, errnum_invalidip, errnum_notfound = 0, 0, 0
cur_source = 'applog'
for outline in hdfs.stdout:
  rownum += 1
  if (rownum%200000)==0:
    logger.debug("row> %d" % rownum)
    #break #测试代码。只取部分数据处理，以便快速进入下一步
  outrow = outline.strip('\n').split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  if outrow[c_userid] in ['',' ','0','-']: 
    errnum_nulluser += 1
    errnum += 1
    continue
  cur_lasttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(outrow[c_requesttime])))    
  cur_lastip = outrow[c_userip].strip(' ')
  cur_userid = outrow[c_userid]
  if not isipaddrv4(cur_lastip):
    errnum_invalidip += 1
    errnum += 1
    continue
  if cur_lastip in iptableset:
    cur_city = iptableset[cur_lastip]['city']
    cur_province = iptableset[cur_lastip]['province']
    cur_country = iptableset[cur_lastip]['country']
  else:
    try:
      ipinfo = ipreader.city(cur_lastip)
    except geoip2.errors.AddressNotFoundError:
      errnum_notfound += 1
      errnum += 1
      iptableset[cur_lastip] = {}
      iptableset[cur_lastip]['ip'] = cur_lastip
      iptableset[cur_lastip]['city'] = unknownstr
      iptableset[cur_lastip]['province'] = unknownstr
      iptableset[cur_lastip]['country'] = unknownstr
      continue
    if ipinfo.city.name is None:
      errnum_notfound += 1
      errnum += 1
      iptableset[cur_lastip] = {}
      iptableset[cur_lastip]['ip'] = cur_lastip
      iptableset[cur_lastip]['city'] = unknownstr
      iptableset[cur_lastip]['province'] = unknownstr
      iptableset[cur_lastip]['country'] = unknownstr
      continue
    #名字中带"`"字符的文件加载HDFS时报错 unexpected URISyntaxException，转为同义的"'"字符
    cur_country = ipinfo.country.name.replace(" ","").replace("`","'") if ipinfo.country.name=='China' else unknownstr
    cur_province = ipinfo.subdivisions.most_specific.name.replace(" ","").replace("`","'") if ipinfo.country.name=='China' else unknownstr
    cur_city = ipinfo.city.name.replace(" ","").replace("`","'") if ipinfo.country.name=='China' else unknownstr
    iptableset[cur_lastip] = {}
    iptableset[cur_lastip]['ip'] = cur_lastip
    iptableset[cur_lastip]['city'] = cur_city
    iptableset[cur_lastip]['province'] = cur_province
    iptableset[cur_lastip]['country'] = cur_country
  cur_key = '$' + cur_country + '$' + cur_province + '$' + cur_city
  if cur_key not in resultset:
    resultset[cur_key] = {}
    resultset[cur_key]['city'] = cur_city
    resultset[cur_key]['province'] = cur_province
    resultset[cur_key]['country'] = cur_country
    resultset[cur_key]['users'] = {}
    resultset[cur_key]['users'][cur_userid] = {}
    resultset[cur_key]['users'][cur_userid]['userid'] = cur_userid
    resultset[cur_key]['users'][cur_userid]['lastip'] = cur_lastip
    resultset[cur_key]['users'][cur_userid]['lasttime'] = cur_lasttime
    resultset[cur_key]['users'][cur_userid]['source'] = cur_source
  else:
    if cur_userid not in resultset[cur_key]['users']:
      resultset[cur_key]['users'][cur_userid] = {}
      resultset[cur_key]['users'][cur_userid]['userid'] = cur_userid
      resultset[cur_key]['users'][cur_userid]['lastip'] = cur_lastip
      resultset[cur_key]['users'][cur_userid]['lasttime'] = cur_lasttime
      resultset[cur_key]['users'][cur_userid]['source'] = cur_source
    elif resultset[cur_key]['users'][cur_userid]['lasttime']<cur_lasttime:
      resultset[cur_key]['users'][cur_userid]['lastip'] = cur_lastip
      resultset[cur_key]['users'][cur_userid]['lasttime'] = cur_lasttime
      resultset[cur_key]['users'][cur_userid]['source'] = cur_source
hdfs.terminate()
ipreader.close()
logger.debug("statistics info (logs.log_php_app_log)\n rownum: %d\n errnum: %d" % (rownum,errnum))
logger.debug(" Null User: %d\n Invalid IP: %d\n Not Found: %d" % (errnum_nulluser,errnum_invalidip,errnum_notfound))

#推送处理部分
if postmode:
  logger.debug("connecting mysql ...")
  dbconn = MySQLdb.connect(host='10.0.10.85',user='bi',passwd='bi_haodou',port=3306,charset='utf8')
  sqlcursor = dbconn.cursor()
  #加载达人数据 #排除菜谱专辑达人、生活盟主、豆友会掌柜、作品达人
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
  viplist = []
  for item in dataset:
    viplist.append(int(item[0]))
  logger.debug("vip: %s" % str(viplist))
  #加载新人数据
  logger.debug("loading new user ...")
  newlimit = 10000
  sqlstmt = r"""
  select min(userid)
  from haodou_passport.User
  where status=1 and regtime >='%(preday7_date)s 00:00:00'
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
  newlimit = int(sqlcursor.fetchone()[0])
  logger.debug("newlimit: %d" % newlimit)
  sqlcursor.close()
  dbconn.commit()
  dbconn.close()
  #推送开始
  httperrnum, httperrlimit = 0, 3
  filechunk = []
  logger.debug("sending data ...")
  #分城市推送
  for city in resultset:
    cur_city = resultset[city]['city']
    cur_province = resultset[city]['province']
    cur_country = resultset[city]['country']
    #简化结果集（格式与推送接口约定保持一致）
    simpleset, top = {}, 200
    user_num, vipuser_num, newuser_num = 0, 0, 0
    simpleset['city'] = cur_city
    simpleset['user'] = []
    simpleset['vipuser'] = []
    simpleset['newuser'] = []
    for user in resultset[city]['users']:
      userid = int(user)
      if user_num>=top and vipuser_num>=top and newuser_num>=top:
        break
      if vipuser_num<top and userid in viplist:
        simpleset['vipuser'].append(userid)
        vipuser_num += 1
      elif newuser_num<top and userid>newlimit:
        simpleset['newuser'].append(userid)
        newuser_num += 1
      elif user_num<top:
        simpleset['user'].append(userid)
        user_num += 1
    simpledata = json.dumps(simpleset)
    #推送动作
    posturl = 'http://211.151.151.230/data/usrecomm'
    httpconn = httplib.HTTPConnection(urlparse(posturl).netloc, timeout=10)
    headers = {'Host':'search.haodou.com', 'Accept-Charset':'UTF-8'}
    postdata = simpledata
    filechunk.append(postdata)
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

#将推送数据写入临时文件
if postmode:
  tmpfilename = "%(file)s_post.dat" % {'file':runfilename}
  tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
  tmpwriter = open(tmpfile,'w')
  try:
    tmpwriter.writelines('\n'.join(filechunk))
  except:
    tmpwriter.close()

#输出结果集格式定义（与结果表bing.dw_haodou_cityuser 结构一致）
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = \
  "%(city)s" + fieldsdelimiter + \
  "%(province)s" + fieldsdelimiter + \
  "%(country)s" + fieldsdelimiter + \
  "%(userid)s" + fieldsdelimiter + \
  "%(lastip)s" + fieldsdelimiter + \
  "%(lasttime)s" + fieldsdelimiter + \
  "%(source)s" + rowsdelimiter

#输出结果集文件
for city in resultset:
  pos, chunksize, resultchunk = 0, 200000, []
  cur_city = resultset[city]['city']
  cur_province = resultset[city]['province']
  cur_country = resultset[city]['country']
  #分城市写结果文件
  tmpfilename = "cityuser_%(city)s_%(province)s.dat" % {'city':cur_city, 'province':cur_province}
  tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
  logger.debug("writing file (%(file)s)..." % {'file':tmpfilename})
  tmpwriter = open(tmpfile,'w')
  for userid in resultset[city]['users']:
    pos += 1
    cur_userid = resultset[city]['users'][userid]['userid']
    cur_lastip = resultset[city]['users'][userid]['lastip']
    cur_lasttime = resultset[city]['users'][userid]['lasttime']
    cur_source = resultset[city]['users'][userid]['source']
    resultline = resultrowformat % { \
      'city':cur_city, \
      'province':cur_province, \
      'country':cur_country, \
      'userid':cur_userid, \
      'lastip':cur_lastip, \
      'lasttime':cur_lasttime, \
      'source':cur_source \
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
  if os.path.exists(tmpfile+".lzo"):
    runShell("rm %s.lzo" % tmpfile)
  runShell("cd %(tmpdir)s && lzop %(tmpfilename)s" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
  #加载到hdfs的外部表目录下 该目录由结果表bing.dw_haodou_cityuser建表语句指定
  logger.debug("loading hdfs ...")
  runShell("hdfs dfs -moveFromLocal -f %(file)s.lzo /bing/ext/dw_haodou_cityuser" % {'file':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)
