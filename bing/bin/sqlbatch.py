#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime, time
import math
import string, codecs
import subprocess

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),"%Y%m%d")
    return True
  except:
    return False

reload(sys)
sys.setdefaultencoding("utf8")

##runtime variable
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.split(os.path.abspath(__file__))[1].split('.')[0]

print "SQLBatch Begin ... "

if len(sys.argv)!=4:
  print "ERROR: missing arg."
  print "Usage: sqlbatch.py <shellcmd> <begindate> <enddate>"
  print "\t\t shellcmd inculde #yyyymmdd or #yyyy-mm-dd"
  print "\t\t date format: yyyymmdd or yyyy-mm-dd"
  sys.exit (-1)

if (sys.argv[1].find("#yyyymmdd")==-1 and sys.argv[1].find("#yyyy-mm-dd")==-1):
  print "ERROR: shellcmd need inculde #yyyymmdd or #yyyy-mm-dd."
  sys.exit (-1)

if not (isdate(sys.argv[2]) and isdate(sys.argv[3])):
  print "ERROR: date format error. yyyymmdd or yyyy-mm-dd"
  sys.exit (-1)

shellcmd = sys.argv[1]
begindate = sys.argv[2]
enddate = sys.argv[3]

dt_begindate = datetime.datetime.strptime(begindate,"%Y%m%d")
dt_enddate = datetime.datetime.strptime(enddate,"%Y%m%d")
dt_statisdate = dt_begindate

while True:
  if dt_statisdate > dt_enddate:
    break
  cmdline = shellcmd
  cmdline = cmdline.replace('#yyyymmdd',dt_statisdate.strftime("%Y%m%d"))
  cmdline = cmdline.replace('#yyyy-mm-dd',dt_statisdate.strftime("%Y-%m-%d"))
  print cmdline
  subprocess.call(cmdline, shell=True)
  dt_statisdate += datetime.timedelta(days=1)

print "SQLBatch End."
