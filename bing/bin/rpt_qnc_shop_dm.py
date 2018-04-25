#!/usr/bin/env python
#encoding: utf-8

import subprocess, sys, os
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser

usageinfo = "%prog [--date=statisdate]"
today = datetime.today().strftime("%Y-%m-%d")

parser = OptionParser(usage=usageinfo, version="%prog 1.0")
parser.set_defaults(statisdate=(datetime.strptime(today,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d"))
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")

(options, args) = parser.parse_args()
statis_date = options.statisdate
#statis_date = datetime.datetime.strptime(statisdate,"%Y%m%d").strftime("%Y-%m-%d")
poststatis_date = (datetime.strptime(statis_date, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
preday7_date = (datetime.strptime(statis_date, '%Y-%m-%d') + timedelta(days=-7)).strftime('%Y-%m-%d')

pwd = os.path.dirname(os.path.realpath(sys.argv[0]))
cmd_template = '%s --sql=%s --date=%s'

cur = preday7_date
while cur != poststatis_date:
    cmd = cmd_template % (os.path.join(pwd, 'sqlexec.py'), os.path.join(pwd, 'sql', 'sp_rpt_qnc_shop_dm.sql'), cur)
    print cmd
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print out
        print err
        sys.exit(p.returncode)
    print "OK"
    print out
    print err
    cur = (datetime.strptime(cur, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

sys.exit(0)
