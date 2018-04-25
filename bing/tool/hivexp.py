#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import logging
from optparse import OptionParser
import math
import string, codecs
import subprocess
import re
import xlwt
import zipfile

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

def isint(s):
  ret = re.match(r'[+-]?\d*$', s)
  return True if ret!=None else False

def isfloat(s):
  ret = re.match(r'[+-]?\d*\.\d*$', s)
  return True if ret!=None else False

def mkfile(s, m):
  if not os.path.exists(s):
    f=open(s,'w')
    f.close()
    os.chmod(s, m)

def delbom(s):
  if s[0]==codecs.BOM_UTF8:
    return s[1:]
  else:
    return s

def delctrlchr(s, cc = "\n\t\r"):
  return str(s).translate(string.maketrans(cc," "*len(cc)))

def sqlescape(s):
  try:
    ret = str(s).replace('\\','\\\\')
    ret = ret.replace('`','\\`')
    return ret
  except:
    return None

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
logger = logging.getLogger("hivexp")
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
usageinfo = "%prog --sql=sqlfile [--out=outputfile] [-v]"
parser = OptionParser(usage=usageinfo, version="%prog v0.3.0")
parser.add_option("-s", "--sql", dest="sqlfile", help="sql file", metavar="FILE")
parser.add_option("-o", "--output", dest="outputfile", help="output file. default <sqlfilename>_<sysdate>.txt", metavar="FILE")
parser.add_option("-z", "--zip", action="store_true", dest="zipmode", default=False, help="zip mode", metavar="MODE")
parser.add_option("-v", "--verbose", action="store_true", dest="verbosemode", default=False, help="verbose mode", metavar="MODE")

(options, args) = parser.parse_args()
sqlfile = options.sqlfile
outputfile = options.outputfile
zipmode = options.zipmode
verbosemode = options.verbosemode

if verbosemode==True:
  consoleHandler.setLevel(logging.DEBUG)

if sqlfile==None:
  logger.error("SQL File is missing.")
  sys.exit(-101)

if os.path.isfile(sqlfile)==False:
  logger.error("SQL File is missing. %s" % sqlfile)
  sys.exit(-102)

if os.access(sqlfile,os.R_OK)==False:
  logger.error("SQL File is not readable. %s" % sqlfile)
  sys.exit(-103)

default_outputfilename = os.path.splitext(sqlfile)[0]
default_ext = '.txt'
if outputfile==None:
  outputfile = '%(dir)s%(sep)s%(filename)s_%(rundate)s%(ext)s' % {'dir':rundir,'sep':os.sep,'filename':default_outputfilename,'rundate':rundate,'ext':default_ext}
outputfilename = os.path.splitext(outputfile)[0]
ext = os.path.splitext(os.path.split(os.path.abspath(outputfile))[1])[1]

if os.path.exists(outputfile):
  if os.access(outputfile,os.W_OK)==False:
    logger.error("Output File is not writeable. %s" % outputfile)
    sys.exit(-104)

jobname = "hivexp@%(script)s" % {'script':os.path.split(os.path.abspath(sqlfile))[1]}
hiveconf = {'mapred.job.name':jobname, 'mapred.job.queue.name':'default', 'hive.cli.print.header':'true'}
conf = ''
for item in hiveconf:
  conf += '--hiveconf %(key)s=%(value)s ' % {'key':item, 'value':hiveconf[item]}
hivebin = "hive -S %(hiveconf)s -e" % {'rundir':rundir, 'hiveconf':conf}

##read sql file
sqlhandle = codecs.open(sqlfile,'r','utf-8')
sqlstmt = sqlhandle.readlines()
sqlhandle.close()
sqlstmt = delbom(sqlstmt)

##sqlstmt analysis
SQL_PRESET_KEYWORD = ["set","add jar","create temporary function"]
SQL_EXECUTE_KEYWORD = ["create table if not exists","insert","select","alter table"]
SQL_KEYWORD = SQL_PRESET_KEYWORD + SQL_EXECUTE_KEYWORD
SQL_COMMENT = "--"
SQL_LINE_END = ";"
preset_section = ""
exec_section = []
sql_line = ""
for line in sqlstmt:
  line = line.lstrip().rstrip()
  idx = line.lower().find(SQL_COMMENT)
  if idx!=-1:
    line = line[:idx].rstrip()
  if line=="":
    continue
  sql_line = sql_line + line + " "
  if line[-1]!=SQL_LINE_END:
    continue
  for keyword in SQL_PRESET_KEYWORD:
    idx = sql_line.lower().find(keyword)
    if idx==0:
      preset_section = preset_section + sql_line + "\n";
      sql_line = ""
      continue
  for keyword in SQL_EXECUTE_KEYWORD:
    idx = sql_line.lower().find(keyword)
    if idx==0:
      exec_section.append(sql_line)
      sql_line = ""
      continue
logger.debug(">>%s >>%s" % (preset_section, exec_section))

if ext=='.xls':
  wb = xlwt.Workbook()
  defaultfont = xlwt.Font()
  defaultfont.height = 0x0118 #14 points
  defaultfont.bold = False
  tabheadfont = xlwt.Font()
  tabheadfont.height = 0x0118 #14 points
  tabheadfont.bold = True
  defaultborders = xlwt.Borders()
  defaultborders.left = xlwt.Borders.THIN
  defaultborders.right = xlwt.Borders.THIN
  defaultborders.top = xlwt.Borders.THIN
  defaultborders.bottom = xlwt.Borders.THIN
  defaultstyle = xlwt.XFStyle()
  defaultstyle.font = defaultfont
  defaultstyle.borders = defaultborders
  defaultstyle.num_format_str = 'general'
  tabheadstyle = xlwt.XFStyle()
  tabheadstyle.font = tabheadfont
  tabheadstyle.borders = defaultborders

tabnum = 0
for sql_line in exec_section:
  sql = preset_section + sql_line
  logger.debug("%s" % sql)
  ##execute sql
  cmd = '%(bin)s "%(sql)s"' % {'bin':hivebin, 'sql':sqlescape(sql)}
  proc = subprocess.Popen(cmd, shell=True, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
  hiveout, errmsg = proc.communicate()
  retval = proc.wait()
  if retval!=0:
    logger.error("HiveError!!!(%d)" % retval)
    logger.error("Debug Info: %s" % str(errmsg))
    sys.exit(retval)
  ##export file
  logger.debug("%s" % hiveout)
  if sql_line.lower().find("select")==0:
    tabnum += 1
    logger.debug("writing file...")
    if (ext=='.txt' or ext=='.dat'):
      if tabnum==1:
        fp = open(outputfile, "w")
      else:
        fp = open(outputfile, "a")
      try:
        fp.write("###%d###\n" % tabnum)
        fp.writelines(hiveout)
      finally:
        fp.close()
    elif ext=='.csv':
      hiveout = hiveout.replace("\t",",")
      if tabnum==1:
        fp = open(outputfile, "w")
      else:
        fp = open(outputfile, "a")
      try:
        fp.write("###%d###\n" % tabnum)
        fp.writelines(hiveout)
      finally:
        fp.close()
    elif ext=='.xls':
      ws = wb.add_sheet("sheet%d" % tabnum)
      rownum = 0
      curstyle = tabheadstyle
      outlines = hiveout.splitlines()
      for outline in outlines:
        if outline[:5]=="WARN:":
          continue
        outrow = outline.split("\t")
        for colnum in range(len(outrow)):
          #print "[%d:%d] %s" % (rownum, colnum, outrow[colnum])
          if outrow[colnum].startswith('http://'):
            content = 'HYPERLINK("%(url)s";"%(url)s")' % {'url':outrow[colnum]}
            ws.write(rownum, colnum, xlwt.Formula(content), curstyle)
          elif outrow[colnum]=='':
            ws.write(rownum, colnum, '', curstyle)
          elif isint(outrow[colnum]):
            ws.write(rownum, colnum, int(outrow[colnum]), curstyle)
          elif isfloat(outrow[colnum]):
            ws.write(rownum, colnum, float(outrow[colnum]), curstyle)
          else:
            ws.write(rownum, colnum, outrow[colnum].decode('utf-8'), curstyle)
        rownum += 1
        if rownum==1:
          curstyle = defaultstyle
    else:
      logger.info("unsupported fileformat.(%s)" % ext)

if ext=='.xls':
  wb.save(outputfile)

##zip
if zipmode:
  logger.debug("zipping file...")
  f = zipfile.ZipFile(outputfilename+".zip","w",zipfile.ZIP_DEFLATED,allowZip64="True")
  f.write(outputfile)
  f.close()

logger.info("end.(%d)" % retval)
sys.exit(retval)
