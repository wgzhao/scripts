#!/usr/bin/env python
#encoding: utf-8

import os, datetime, commands, sys
from optparse import OptionParser

usageinfo = "%prog [--date=statisdate]"
rundate = datetime.datetime.today().strftime("%Y%m%d")
parser = OptionParser(usage=usageinfo, version="%prog 1.0")
parser.set_defaults(statisdate=(datetime.datetime.strptime(rundate,"%Y%m%d")+datetime.timedelta(days=-1)).strftime("%Y%m%d"))
#parser.set_defaults(verbosemode=False)
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")

(options, args) = parser.parse_args()
statisdate = options.statisdate.replace('-','')
statis_date = datetime.datetime.strptime(statisdate,"%Y%m%d").strftime("%Y-%m-%d")
preday30_date = (datetime.datetime.strptime(statisdate,"%Y%m%d") + datetime.timedelta(days=-30)).strftime('%Y-%m-%d')
curdate = datetime.datetime.today().strftime('%Y%m%d')

sql = ''
with open(os.path.split(os.path.abspath(__file__))[0] + '/sql/sp_rpt_qnc_infocompletedist_mm.sql', 'rb') as f:
    s = f.read()
    sql = s.replace('${statis_date}', statis_date) \
        .replace('${statisdate}', statisdate) \
        .replace('${preday30_date}', preday30_date) \
        .replace('${curdate}', curdate)
    print sql

cmd = """/opt/app/spark/bin/spark-sql --jars hdfs://hdcluster/udf/haodoubihiveudf.jar -f /tmp/lifangxing/sp_rpt_qnc_infocompletedist_mm.sql"""

#print sql
if not os.path.exists('/tmp/lifangxing'):
    os.mkdir('/tmp/lifangxing')
with open('/tmp/lifangxing/sp_rpt_qnc_infocompletedist_mm.sql', 'w') as f:
    f.write(sql)

status, ret = commands.getstatusoutput(cmd)
if status != 0:
    sys.stderr.write(ret+'\n')

sys.exit(status)
