#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import logging
from optparse import OptionParser
import math
import string, codecs
import subprocess

def freshdefaultswrapper(f):
  fdefaults = f.func_defaults
  def refresher(*args, **kwds):
    f.func_defaults = copy.deepcopy(fdefaults)
  return refresher

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
    os.chmod(s, 0666)

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

def export_mysql(psql, pfile):
  export_cmd='mysql -uwebuser -ponlyhaodou321 -h10.0.11.50 -P3306 -N -s -e "set names ''utf8'';%(sql)s" > ./%(datfile)s' % {'sql':delctrlchr(psql),'datfile':pfile}
  logger.debug("%s" % export_cmd)
  proc = subprocess.Popen(export_cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  retmsg, errmsg = proc.communicate()
  logger.debug("%s" % str(retmsg))
  retval = proc.wait()
  if retval!=0:
    logger.error("CalledProcessError!!!(mysql,%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)

def load_hive(ptable, pfile):
  load_cmd='hive -S -e "load data local inpath \'./%(datfile)s\' overwrite into table %(table)s;"' % {'datfile':pfile,'table':ptable}
  logger.debug("%s" % load_cmd)
  proc = subprocess.Popen(load_cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  retmsg, errmsg = proc.communicate()
  logger.debug("%s" % str(retmsg))
  retval = proc.wait()
  if retval!=0:
    logger.error("CalledProcessError!!!(hive,%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)

reload(sys)
sys.setdefaultencoding('utf-8')

##runtime variable
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
homedir= rundir + "/.."
logdir = homedir + "/log"
tmpdir =  homedir + "/tmp"
if not os.path.exists(logdir):
  os.mkdir(logdir,0777)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0777)
logfile = '%(dir)s%(sep)s%(filename)s.log' % {'dir':logdir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

##logger
logger = logging.getLogger("showdb2hive")
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(logfile)
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(levelname)s - %(message)s"))
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d - %(message)s"))
logger.addHandler(consoleHandler)

logger.info("begin execute... %s" % str(sys.argv))

##option parse
usageinfo = "%prog [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.0.1")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
verbosemode = options.verbosemode

if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

tablename = 'bing.rpt_hoto_reguser_dm_v2'
logger.info("%s..." % tablename)
sqlstmt = R"""
select statis_date, total_user 
from showdb.rpt_user_index
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)

tablename = 'bing.rpt_hoto_avgdev_mm_v3'
logger.info("%s..." % tablename)
sqlstmt = R"""
select concat(substr(statis_date,1,7),'-01'), round(avg(app_user)) 
from showdb.rpt_dayact_index 
where app_user>0 
group by substr(statis_date,1,7)
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)

tablename = 'bing.rpt_hoto_actdev_dm_v3'
logger.info("%s..." % tablename)
sqlstmt = R"""
select statis_date, app_user
from showdb.rpt_dayact_index 
where app_user>0
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)

tablename = 'bing.rpt_hoto_actdev_mm_v3'
logger.info("%s..." % tablename)
sqlstmt = R"""
select concat(substr(statis_date,1,7),'-01'), 
max(android_user)+max(iphone_user),
max(android_user), 
max(iphone_user) 
from showdb.rpt_monthact_index 
where android_user>0 
group by substr(statis_date,1,7)
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)


tablename = 'bing.rpt_hoto_actdev_wm_v3'
logger.info("%s..." % tablename)
sqlstmt = R"""
select d.statis_date,
round(case when d.days<8 then 0.56 when d.days<15 then 0.58 when d.days<22 then 0.57 else 0.59 end*t.devs),
round(case when d.days<8 then 0.56 when d.days<15 then 0.58 when d.days<22 then 0.57 else 0.59 end*t.adev),
round(case when d.days<8 then 0.56 when d.days<15 then 0.58 when d.days<22 then 0.57 else 0.59 end*t.idev)
from
(select substr(statis_date,1,7) as statis_month, statis_date, day(statis_date) as days
from showdb.rpt_monthact_index
where android_user>0 and dayofweek(statis_date)=1 
) d,
(select substr(statis_date,1,7) as statis_month, 
max(android_user)+max(iphone_user) as devs,
max(android_user) as adev, 
max(iphone_user) as idev
from showdb.rpt_monthact_index 
where android_user>0 
group by substr(statis_date,1,7)
) t
where d.statis_month=t.statis_month
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)


tablename = 'bing.rpt_hoto_avgdur_dm'
logger.info("%s..." % tablename)
sqlstmt = R"""
select statis_date, 0, appdur 
from showdb.rpt_appdur_index
;
"""
datafile = '%s_%s.dat' % (tablename, rundate)
export_mysql (sqlstmt, datafile)
load_hive (tablename, datafile)

logger.info("execute end.")
