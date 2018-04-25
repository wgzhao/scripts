#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime, time
import string, codecs
import subprocess
import urlparse
import re

_OFF, _FATAL, _ERROR, _WARN, _INFO, _DEBUG, _TRACE, _ALL = range(8)
DEBUG_LEVEL = _DEBUG

def print_error(*arg):
  global DEBUG_LEVEL
  if len(arg)!=0 and DEBUG_LEVEL>=_ERROR:
    for a in arg: print a,
    print
  else:
    pass

def print_info(*arg):
  global DEBUG_LEVEL
  if len(arg)!=0 and DEBUG_LEVEL>=_INFO:
    for a in arg: print a,
    print
  else:
    pass

def print_debug(*arg):
  global DEBUG_LEVEL
  if len(arg)!=0 and DEBUG_LEVEL>=_DEBUG:
    for a in arg: print a,
    print
  else:
    pass

def print_trace(*arg):
  global DEBUG_LEVEL
  if len(arg)!=0 and DEBUG_LEVEL>=_TRACE:
    for a in arg: print a,
    print
  else:
    pass

def delbom(s):
  if s[0]==codecs.BOM_UTF8:
    return s[1:]
  else:
    return s

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),"%Y%m%d")
    return True
  except:
    return False

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

def runShell(cmd):
  print_debug("shell# %s" % str(cmd))
  proc = subprocess.Popen(str(cmd), shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  shellout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    print_error("ShellError!!!(%d) %s" % (retval, str(cmd)))
    print_debug("Debug Info: %s" % str(errmsg))
    sys.exit(-1)
  return shellout

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

def sqlEscape(s):
  try:
    ret = str(s).replace('\\','\\\\')
    ret = ret.replace('`','\\`')
    return ret
  except:
    return None

def runHiveQL(sql):
  print_debug("sql# %s" % sql)
  cmd = 'hive -S -e "%(sql)s"' % {'sql':sqlEscape(sql)}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  hiveout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    print_error("HiveError!!!(%d)" % retval)
    print_debug("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return hiveout

def runMySQL(sql, connstr):
  print_debug("sql# %s" % sql)
  cmd = 'mysql -N -B -e "%(sql)s" %(conn)s' % {'sql':sqlEscape(sql), 'conn':connstr}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  mysqlout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    print_error("MysqlError!!!(%d)" % retval)
    print_debug("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  return mysqlout

def geturl(url):
  s = str(url)
  n1 = s.find('?')
  n2 = s.find('#')
  if n1!=-1 and n2!=-1:
    s = s[0:(n1 if n1<n2 else n2)]
  elif n1==-1 or n2==-1:
    s = s[0:(n2 if n1==-1 else n1)]
  return s

def delspamchr(s, cc = "~!@#$%^&*"):
  return str(s).translate(string.maketrans(cc," "*len(cc))).replace(" ","")

reload(sys)
sys.setdefaultencoding('utf8')

pid = os.getpid()
rundate = datetime.date.today().strftime('%Y%m%d')
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
tmpdir =  rundir + '/tmp'
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0774)

#参数检查
if len(sys.argv) != 2:
  print_error("Usage: %s [date]" % sys.argv[0])
  sys.exit(-100)

opt_statisdate = sys.argv[1]
if isdate(opt_statisdate)==False:
  print_error("unconverted date %s" % opt_statisdate)
  sys.exit(-101)

#内部变量
statisdate = opt_statisdate.replace('-','')
dt_statisdate = datetime.datetime.strptime(statisdate,'%Y%m%d')
statis_date = dt_statisdate.strftime('%Y-%m-%d')

#垃圾关键字
SPAMWORDS = '三挫仑,失忆水,麻醉药,乙醚,安乐死,苍蝇水,春药,杜冷丁,听话药,听话水,ghb,迷情,迷昏,乖乖药,乖乖水'
SPAMWORDLIST = SPAMWORDS.split(',')

#数据来源
outfieldnum=4
c_userid, c_fooddiaryid, c_title, c_content = range(outfieldnum)

sqlstmt = R"""
set hive.cli.print.header=false;
set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;

select userid, fooddiaryid, title, content
from haodou_center_${rundate}.UserDiary
where createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
;

sql = delctrlchr(sqlstmt,'\t\r')
sql = sql.replace('${rundate}',rundate)
sql = sql.replace('${statis_date}',statis_date)
print_debug("sql# %s" % sql)
cmd = 'hive -S -e "%(sql)s"' % {'sql':sqlEscape(sql)}
hive = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
"""

sqlstmt = R"""
select userid, fooddiaryid, title, content
from haodou_center.UserDiary
where createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
;
"""

sql = delctrlchr(sqlstmt,'\t\r')
sql = sql.replace('${rundate}',rundate)
sql = sql.replace('${statis_date}',statis_date)
print_debug("sql# %s" % sql)
connstr = "-ubi -pbi_haodou -h10.1.1.70 -P3306"
cmd = 'mysql -N -B -e "%(sql)s" %(conn)s' % {'sql':sqlEscape(sql), 'conn':connstr}
mysql = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  

spamuser = {}
spampost = {}
rownum, errnum, spamnum = 0, 0, 0
for outline in mysql.stdout:
  rownum += 1
  outrow = outline.strip('\n').split("\t")
  if len(outrow)< outfieldnum:
    errnum += 1
    continue
  cur_userid = outrow[c_userid]
  cur_fooddiaryid = outrow[c_fooddiaryid]
  cur_title = delspamchr(outrow[c_title])
  cur_content = delspamchr(outrow[c_content])
  for keyword in SPAMWORDLIST:
    if cur_title.find(keyword)!=-1 or cur_content.find(keyword)!=-1:
      spamnum += 1
      if cur_userid not in spamuser:
        spamuser[cur_userid] = {}
        spamuser[cur_userid]['userid'] = cur_userid
        spamuser[cur_userid]['postnum'] = 1
      else:
        spamuser[cur_userid]['postnum'] += 1
      if cur_fooddiaryid not in spampost:
        spampost[cur_fooddiaryid] = {}
        spampost[cur_fooddiaryid]['userid'] = cur_userid
        spampost[cur_fooddiaryid]['postid'] = cur_fooddiaryid
        spampost[cur_fooddiaryid]['title'] = cur_title
        spampost[cur_fooddiaryid]['content'] = cur_content
      break
mysql.terminate()
print_info('statistics info')
print_info("rownum: %d" % rownum)
print_info("errnum: %d" % errnum)
print_info("spamnum: %d" % spamnum)

#结果输出1
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = "%(userid)s" + rowsdelimiter
resultchunk = []
for user in spamuser:
  resultline = resultrowformat % { \
    'userid':spamuser[user]['userid'], \
    'postnum':spamuser[user]['postnum'] \
    }
  resultchunk.append(resultline)
tmpfilename = "spamuser_%(statisdate)s.txt" % {'statisdate':statisdate}
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

#结果输出2
fieldsdelimiter, rowsdelimiter = '\t', '\n'
resultrowformat = "%(postid)s" + rowsdelimiter
resultchunk = []
for post in spampost:
  resultline = resultrowformat % { \
    'userid':spampost[post]['userid'], \
    'postid':spampost[post]['postid'], \
    'title':spampost[post]['title'], \
    'content':spampost[post]['content'] \
    }
  resultchunk.append(resultline)
tmpfilename = "spampost_%(statisdate)s.txt" % {'statisdate':statisdate}
tmpfile = "%(dir)s/%(tmpfilename)s" % {'dir':tmpdir, 'tmpfilename':tmpfilename}
tmpwriter = open(tmpfile,'w')
tmpwriter.writelines(resultchunk)
tmpwriter.close()

print_info('ok')
sys.exit(0)
