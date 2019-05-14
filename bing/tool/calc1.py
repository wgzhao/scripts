#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, datetime
import string, codecs
import re

def delbom(s):
  if s[0]==codecs.BOM_UTF8:
    return s[1:]
  else:
    return s

def isnum(s):
  ret = re.match(r'[+-]?\d*[\.]?\d*$', s)
  return True if ret!=None else False

reload(sys)
sys.setdefaultencoding('utf8')

#参数检查
if len(sys.argv) != 2:
  print "Usage: python %s [datafile]" % sys.argv[0]
  sys.exit(-100)

datafile = sys.argv[1]
if not os.path.exists(datafile):
  print "DataFile is missing. %s" % datafile
  sys.exit(-101)
if os.path.isfile(datafile)==False:
  print "DataFile is missing. %s" % datafile
  sys.exit(-102)
if os.access(datafile,os.R_OK)==False:
  print "DataFile is not readable. %s" % datafile
  sys.exit(-103)

#数据文件格式定义
e_function_id, \
e_dur = range(2)
coldel = '\t'

#读取文件
datahandle = codecs.open(datafile,'r','utf-8')
data = datahandle.readlines()
datahandle.close()
data = delbom(data)

#处理数据
resultset = {}
progbar, proglen, progidx = sys.stdout.isatty(), len(data), 0
barwidth, barpos, bartime = 40, 0, datetime.datetime.now()
for line in data:
  if progbar:
    curtime = datetime.datetime.now()
    progidx += 1
    progperc = 100. * progidx / proglen
    barpos = int(barwidth * progperc / 100)
    if progidx==1 or progidx==proglen or (curtime - bartime).seconds>=1:
      bartime = curtime
      print '\rrunning... [' + '#'*barpos + '-'*(barwidth-barpos) + ']',
      print '%.2f%%(%d/%d)' % (progperc, progidx, proglen),
      sys.stdout.flush()
  line = line.lstrip().rstrip()
  row = line.split(coldel)
  if len(row)!=2:
    continue
  if len(row[e_function_id])<50 and isnum(row[e_dur]):
    function_id, dur = row[e_function_id], float(row[e_dur])
    if function_id not in resultset:
      resultset[function_id] = {}
      resultset[function_id]['function_id'] = function_id
      resultset[function_id]['sum'] = dur
      resultset[function_id]['count'] = 1
    else:
      resultset[function_id]['sum'] += dur
      resultset[function_id]['count'] += 1
if progbar: print

#结果输出
for key in resultset:
  print '%s\t%.3f\t%d' % (resultset[key]['function_id'], 1.*resultset[key]['sum']/resultset[key]['count'], resultset[key]['count'])

sys.exit(0)
