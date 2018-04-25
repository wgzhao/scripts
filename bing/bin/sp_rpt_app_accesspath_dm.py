#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import logging
from optparse import OptionParser
import math
import string, codecs
import subprocess
import hashlib

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

def list2str(l):
  return "'" + "','".join(l) + "'"

def delCtrlChar(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

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

def runShell(cmd):
  logger.debug("shell# %s" % str(cmd))
  proc = subprocess.Popen(str(cmd), shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  shellout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("ShellError!!!(%d) %s" % (retval, str(cmd)))
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return shellout

def getJsonValString(jsontext,key):
  key = '"%(keyword)s":"' % {'keyword':key}
  p0 = jsontext.find(key)
  if p0==-1:
    return ""
  p0 += len(key)
  p1 = jsontext.find('"',p0)
  if p1==-1:
    return ""
  return jsontext[p0:p1]

reload(sys)
sys.setdefaultencoding("utf8")

retval = 0

##runtime variable
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
logdir = rundir + "/log"
tmpdir =  rundir + "/tmp"
if not os.path.exists(logdir):
  os.mkdir(logdir,0774)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0774)
logfile = '%(dir)s/%(filename)s.log' % {'dir':logdir,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

##logger
logger = logging.getLogger("sp")
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
usageinfo = "%prog [--date=statisdate] [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.0.1")
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,"%Y%m%d")+datetime.timedelta(days=-1)).strftime("%Y%m%d"))
parser.set_defaults(limitcnt="100")
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")
parser.add_option("-l", "--limit", dest="limitcnt", help="limit count, default value is 100", metavar="VALUE")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
opt_statisdate = options.statisdate.replace('-','')
opt_limitcnt = options.limitcnt
opt_verbosemode = options.verbosemode

if isdate(opt_statisdate)==False:
  logger.error("unconverted date %s" % opt_statisdate)
  sys.exit(-101)
statisdate = opt_statisdate

if not opt_limitcnt.isdigit():
  logger.error("unconverted int %s" % opt_limitcnt)
  sys.exit(-102)
limitcnt = int(opt_limitcnt)

if opt_verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
statis_date = dt_statisdate.strftime("%Y-%m-%d")
statisdate = dt_statisdate.strftime("%Y%m%d")
curdate = rundate
cur_date = "%s-%s-%s" % (curdate[0:4],curdate[4:6],curdate[6:8])
nullstring = ''

jobname = "%(script)s@%(statisdate)s" % {'script':os.path.split(os.path.abspath(__file__))[1], 'statisdate':statisdate}
hiveconf = {'mapred.job.name':jobname, 'mapred.job.queue.name':'default'}
conf = ''
for item in hiveconf:
  conf += '--hiveconf %(key)s=%(value)s ' % {'key':item, 'value':hiveconf[item]}
bin = "hive -S %(hiveconf)s -e" % {'rundir':rundir, 'hiveconf':conf}

logger.debug("reading accesslog (%s)..." % statis_date)

##查询输出数据格式
outfieldnum=7
c_app_id     , \
c_dev_uuid   , \
c_channel_id , \
c_ssession_id, \
c_session_seq, \
c_version_id , \
c_page = range(outfieldnum)

##结果数据格式 应与目的表bing.rpt_app_accesspath_dm 的定义格式一致
fieldsdelimiter, rowsdelimiter = "\t", "\n"
resultrowformat = \
"%(app_id)s" + fieldsdelimiter + \
"%(version_id)s" + fieldsdelimiter + \
"%(node_key)s" + fieldsdelimiter + \
"%(node_path)s" + fieldsdelimiter + \
"%(node_page)s" + fieldsdelimiter + \
"%(node_level)d" + fieldsdelimiter + \
"%(subnode_num)d" + fieldsdelimiter + \
"%(access_cnt)d" + fieldsdelimiter + \
"%(bounce_cnt)d" + fieldsdelimiter + \
"%(prior_nodekey)s" + rowsdelimiter

sqlstmt = R"""
set hive.cli.print.header=false;
set mapred.reduce.tasks=15;

select app_id, dev_uuid, channel_id, session_id, session_seq,
version_id, page
from bing.dw_app_accesslog_session_dm
where statis_date='${statis_date}'
and dev_uuid!='' and session_seq<=50
distribute by app_id, dev_uuid
sort by app_id, dev_uuid, channel_id, session_id, session_seq
;
"""
sql = delCtrlChar(sqlstmt,'\t\r')
sql = sql.replace('${statis_date}',statis_date)
logger.debug("sql# %s" % sql)
cmd = '%(bin)s "%(sql)s"' % {'bin':bin, 'sql':sqlEscape(sql)}
hive = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

logger.debug("collecting session ...")
presession, cursession = '', ''
pos, errnum, resultset = 0, 0, {}

##初始化根节点
resultset['NULL'] = {}
resultset['NULL']['app_id'] = ''
resultset['NULL']['version_id'] = ''
resultset['NULL']['node_key'] = 'NULL'
resultset['NULL']['node_path'] = 'NULL'
resultset['NULL']['node_page'] = 'NULL'
resultset['NULL']['node_level'] = 0
resultset['NULL']['subnode_num'] = 0
resultset['NULL']['access_cnt'] = 0
resultset['NULL']['bounce_cnt'] = 0
resultset['NULL']['prior_nodekey'] = ''

for outline in hive.stdout:
  pos += 1
  outrow = outline.strip().split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  if str(outrow[c_session_seq])=='1':
    prekey = 'NULL'
    cur_prior_nodekey = 'NULL'
    cur_app_id = outrow[c_app_id]
    cur_version_id = outrow[c_version_id]
    cur_node_page = outrow[c_page]
    cur_node_path = cur_node_page
    cur_node_key = hashlib.md5(cur_node_path).hexdigest()
    cur_node_level = 1
    cur_subnode_num = 0
    curkey = '$' + cur_app_id + '$' + cur_version_id + '$' + cur_node_key
  else:
    prekey = curkey
    cur_prior_nodekey = resultset[prekey]['node_key']
    cur_app_id = outrow[c_app_id]
    cur_version_id = outrow[c_version_id]
    cur_node_page = outrow[c_page]
    cur_node_path = cur_node_path + ':' + cur_node_page
    cur_node_key = hashlib.md5(cur_node_path).hexdigest()
    cur_node_level = int(outrow[c_session_seq])
    curkey = '$' + cur_app_id + '$' + cur_version_id + '$' + cur_node_key
  if curkey not in resultset:
    resultset[curkey] = {}
    resultset[curkey]['app_id'] = cur_app_id
    resultset[curkey]['version_id'] = cur_version_id
    resultset[curkey]['node_key'] = cur_node_key
    resultset[curkey]['node_path'] = cur_node_path
    resultset[curkey]['node_page'] = cur_node_page
    resultset[curkey]['node_level'] = cur_node_level
    resultset[curkey]['subnode_num'] = 0
    resultset[curkey]['access_cnt'] = 1
    resultset[curkey]['bounce_cnt'] = 1
    resultset[curkey]['prior_nodekey'] = cur_prior_nodekey
    if prekey!='NULL':
      resultset[prekey]['subnode_num'] +=1
  else:
    resultset[curkey]['access_cnt'] += 1
    resultset[curkey]['bounce_cnt'] += 1
  if prekey!='NULL':
    resultset[prekey]['bounce_cnt'] -=1

resultchunk = []
for key in resultset:
  if resultset[key]['access_cnt']<limitcnt:
    continue
  resultline = resultrowformat % { \
    'app_id':resultset[key]['app_id'], \
    'version_id':resultset[key]['version_id'], \
    'node_key':resultset[key]['node_key'], \
    'node_path':resultset[key]['node_path'], \
    'node_page':resultset[key]['node_page'], \
    'node_level':resultset[key]['node_level'], \
    'subnode_num':resultset[key]['subnode_num'], \
    'access_cnt':resultset[key]['access_cnt'], \
    'bounce_cnt':resultset[key]['bounce_cnt'], \
    'prior_nodekey':resultset[key]['prior_nodekey']  \
    }
  resultchunk.append(resultline)

##write to tmpfile
logger.debug("write to tmpfile ...")
tmpfilename = 'rpt_app_accesspath_dm_%(statisdate)s.dat' % {'statisdate':statisdate}
tmpfile = '%(tmpdir)s/%(tmpfilename)s' % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w+')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

logger.debug("zipping file ...")
if os.path.exists(tmpfile+".lzo"):
  runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})
runShell("cd %(tmpdir)s && lzop %(tmpfilename)s" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})

logger.debug("loading data ...")
runHiveQL("load data local inpath '%(tmpfile)s.lzo' overwrite into table bing.rpt_app_accesspath_dm partition (statis_date='%(statis_date)s');" % {'tmpfile':tmpfile, 'statis_date':statis_date})

logger.debug("deleting tmpfile ...")
runShell("cd %(tmpdir)s && ls -l %(tmpfilename)s* | grep -v .list > %(tmpfilename)s.list" % {'tmpdir':tmpdir, 'tmpfilename':tmpfilename})
runShell("rm %(tmpfile)s" % {'tmpfile':tmpfile})
runShell("rm %(tmpfile)s.lzo" % {'tmpfile':tmpfile})

logger.info("end.(%d)" % retval)
sys.exit(retval)
