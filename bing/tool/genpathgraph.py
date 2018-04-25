#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import logging
from optparse import OptionParser
import math
import string, codecs
import subprocess
import MySQLdb
import pydot

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

def chr2underline(s, cc = "."):
  return str(s).translate(string.maketrans(cc,"_"*len(cc)))

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

reload(sys)
sys.setdefaultencoding('utf8')

retval = 0

##runtime variable
pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
logfile = '%(dir)s%(sep)s%(filename)s.log' % {'dir':rundir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mkfile(logfile, 0664)

##logger
logger = logging.getLogger("dotgraph")
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

##option parse
usageinfo = "%prog --app= --ver= [--date=] [--out=] [-s] [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.0.1")
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,"%Y%m%d")+datetime.timedelta(days=-1)).strftime("%Y%m%d"))
parser.set_defaults(limitcnt="1000")
parser.add_option("-a", "--app", dest="appid", help="app id", metavar="STRING")
parser.add_option("-r", "--version", dest="versionid", help="version", metavar="STRING")
parser.add_option("-c", "--channel", dest="channelid", help="channel", metavar="STRING")
parser.add_option("-d", "--date", dest="statisdate", help="statis date. default value is yesterday. yyyymmdd", metavar="DATE")
parser.add_option("-e", "--end", dest="enddate", help="end date. default value is statisdate. yyyymmdd", metavar="DATE")
parser.add_option("-l", "--limit", dest="limitcnt", help="limit count, default value is 1000", metavar="VALUE")
parser.add_option("-o", "--output", dest="outputfile", help="output file. default path_<app>_<version>.png", metavar="FILE")
parser.add_option("-s", "--showdetail", action="store_true", dest="detailmode", default=False, help="showdetail mode", metavar="MODE")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
opt_appid = options.appid
opt_versionid = options.versionid
opt_channelid = options.channelid
opt_statisdate = options.statisdate.replace('-','')
opt_enddate = options.enddate
opt_limitcnt = options.limitcnt
opt_outputfile = options.outputfile
opt_detailmode = options.detailmode
opt_verbosemode = options.verbosemode

if opt_verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

if opt_appid==None:
  logger.error("The app is missing.")
  sys.exit(-101)
versionid = opt_versionid

if not opt_appid.isdigit():
  logger.error("unconverted int %s" % opt_appid)
  sys.exit(-102)
appid = opt_appid

if opt_versionid==None:
  logger.error("The version is missing.")
  sys.exit(-103)
versionid = opt_versionid

if isdate(opt_statisdate)==False:
  logger.error("unconverted date %s" % opt_statisdate)
  sys.exit(-104)
statisdate = opt_statisdate
dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
statis_date = dt_statisdate.strftime("%Y-%m-%d")

if opt_enddate==None:
  isoneday = True
  opt_enddate = opt_statisdate
else:
  isoneday = False
  if isdate(opt_enddate.replace('-',''))==False:
    logger.error("unconverted date %s" % opt_enddate)
    sys.exit(-105)
enddate = opt_enddate.replace('-','')
dt_enddate = datetime.datetime.strptime(enddate,"%Y%m%d")
end_date = dt_enddate.strftime("%Y-%m-%d")

if not opt_limitcnt.isdigit():
  logger.error("unconverted int %s" % opt_limitcnt)
  sys.exit(-105)
limitcnt = int(opt_limitcnt)

default_ext = '.png'
default_outputfilename = "path_%(appid)s_%(version)s_%(date)s" % {'appid':opt_appid, 'version':chr2underline(opt_versionid), 'date':dt_statisdate.strftime("%m%d")}
if opt_outputfile==None:
  outputfile = '%(dir)s%(sep)s%(filename)s%(ext)s' % {'dir':rundir,'sep':os.sep,'filename':default_outputfilename,'ext':default_ext}
else:
  outputfile = opt_outputfile
outputfilename = os.path.splitext(outputfile)[0]
ext = os.path.splitext(os.path.split(os.path.abspath(outputfile))[1])[1]

if ext not in ['.dot','.png','.jpg','.jpeg']:
  logger.error("unsupported fileformat.(%s)" % ext)
  sys.exit(-106)

if os.path.exists(outputfile):
  if os.access(outputfile,os.W_OK)==False:
    logger.error("Output File is not writeable. %s" % outputfile)
    sys.exit(-107)

##读取数据
dbconn = MySQLdb.connect(host='10.0.11.50',user='imp',passwd='imp123',port=3306,db='bing',charset='utf8')
sqlcursor = dbconn.cursor()
sqlcursor.execute("set names 'utf8'")

sqlstmt_one = R"""
select dm.node_key, dm.node_page, dm.node_level, dm.access_cnt, dm.bounce_cnt, dm.prior_nodekey,
round(100.0*dm.bounce_cnt/dm.access_cnt,1) as bounce_rate,
round(100.0*dm.access_cnt/pm.access_cnt,1) as access_percent,
coalesce(pp.page_name,dm.node_page) as page_name
from bing.rpt_app_accesspath_dm dm 
left join (select page_code, page_name from bing.dw_app_page where app_id='${appid}') pp on (dm.node_page=pp.page_code)
left join (select node_key, access_cnt from bing.rpt_app_accesspath_dm
where statis_date='${statis_date}'
and app_id='${appid}' and version_id='${versionid}'
and access_cnt>=${limitcnt}
) pm on (dm.prior_nodekey=pm.node_key)
where dm.statis_date='${statis_date}'
and dm.app_id='${appid}' and dm.version_id='${versionid}'
and dm.access_cnt>=${limitcnt}
order by dm.access_cnt desc
;
"""

sqlstmt_more = R"""
select /*+ mapjoin(dm,ff,pm)*/
dm.node_key, dm.node_page, dm.node_level, dm.access_cnt, dm.bounce_cnt, dm.prior_nodekey,
round(100.0*dm.bounce_cnt/dm.access_cnt,1) as bounce_rate,
round(100.0*dm.access_cnt/pm.access_cnt,1) as access_percent,
coalesce(pp.page_name,dm.node_page) as page_name
from
(select node_key, node_page, node_level, prior_nodekey,
sum(access_cnt) as access_cnt,
sum(bounce_cnt) as bounce_cnt
from bing.rpt_app_accesspath_dm
where statis_date between '${statis_date}' and '${end_date}'
and app_id='${appid}' and version_id='${versionid}'
and access_cnt>=${limitcnt}
group by node_key, node_page, node_level, prior_nodekey
) dm
left join (select page_code, page_name from bing.dw_app_page where app_id='${appid}') pp on (dm.node_page=pp.page_code)
left join (select node_key, sum(access_cnt) as access_cnt
from bing.rpt_app_accesspath_dm
where statis_date between '${statis_date}' and '${end_date}'
and app_id='${appid}' and version_id='${versionid}'
and access_cnt>=${limitcnt}
group by node_key
) pm on (dm.prior_nodekey=pm.node_key)
order by dm.access_cnt desc
;
"""
if isoneday:
  sqlstmt = sqlstmt_one
else:
  sqlstmt = sqlstmt_more
sql = delctrlchr(sqlstmt,'\t\r')
sql = sql.replace('${appid}',appid)
sql = sql.replace('${versionid}',versionid)
sql = sql.replace('${statis_date}',statis_date)
sql = sql.replace('${end_date}',end_date)
sql = sql.replace('${limitcnt}',str(limitcnt))
logger.debug("sql = %s" % sql)
sqlcursor.execute(sql)
resultset = sqlcursor.fetchall()

##画图
if isoneday:
  graph_label = "应用:%(appid)s 版本:%(version)s 访问路径图 //基于%(statisdate)s数据" % {'appid':opt_appid, 'version':opt_versionid, 'statisdate':opt_statisdate}
else:
  graph_label = "应用:%(appid)s 版本:%(version)s 访问路径图 //基于%(statisdate)s - %(enddate)s数据" % {'appid':opt_appid, 'version':opt_versionid, 'statisdate':opt_statisdate, 'enddate':opt_enddate}
graph = pydot.Dot('accesspath', graph_type='digraph', label=graph_label, fontname="Verdana", fontsize=10)
graph.set_node_defaults(shape="record", fixedsize="false", fontname="Verdana", fontsize=10, color="skyblue")
graph.set_edge_defaults(arrowhead="vee", fontname="Verdana", fontsize=9, color="blue")

for (node_key, node_page, node_level, access_cnt, bounce_cnt, prior_nodekey, bounce_rate, access_percent, page_name) in resultset:
  if opt_detailmode:
    node_label = "{<head>%s|访问数:%d|跳出率:%3.1f%%}" % (page_name,access_cnt,bounce_rate)
  else:
    node_label = "{<head>%s|跳出率:%3.1f%%}" % (page_name,bounce_rate)
  graph.add_node(pydot.Node('_%s' % node_key, label=node_label))
  if prior_nodekey!='NULL':
    graph.add_edge(pydot.Edge('_%s' % prior_nodekey, '_%s' % node_key, label="(%3.1f%%)" % access_percent))

if sqlcursor.rowcount==0:
  graph.add_node(pydot.Node('_NOTFOUND', label='!!!无相关数据!!!'))

logger.debug("dot# %s" % graph.to_string())

if ext=='.dot':
  graph.write(outputfile)
elif ext=='.png':
  graph.write_png(outputfile)
elif ext=='.jpg' or ext=='.jpeg':
  graph.write_jpeg(outputfile)
else:
  logger.info("unsupported fileformat.(%s)" % ext)


logger.info("end.(%d)" % retval)
sys.exit(retval)
