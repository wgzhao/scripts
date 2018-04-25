#!/usr/local/bin/python2.7
#encoding: utf-8

import subprocess, sys, os
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser
from StringIO import StringIO
import pandas as pd

usageinfo = "%prog [--date=statisdate]"
today = datetime.today().strftime("%Y-%m-%d")
rtoday = datetime.today().strftime("%Y%m%d")

parser = OptionParser(usage=usageinfo, version="%prog 1.0")
parser.set_defaults(statisdate=(datetime.strptime(today,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d"))
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")

(options, args) = parser.parse_args()
statis_date = options.statisdate
#statis_date = datetime.datetime.strptime(statisdate,"%Y%m%d").strftime("%Y-%m-%d")
poststatis_date = (datetime.strptime(statis_date, '%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
preday7_date = (datetime.strptime(statis_date, '%Y-%m-%d') + timedelta(days=-7)).strftime('%Y-%m-%d')

pwd = os.getcwd()
cmd_template = '%s --sql=%s --date=%s'

def rpt_qnc_paishare_dm(statis_day):
    p=subprocess.Popen("""hive --hiveconf hive.cli.print.header=true -e "select userid,shopid,regexp_replace(content, '\\t', '') content,status,\`from\`,createtime,price from qnc_haodou_pai_%s.paishare where to_date(createtime) = '%s'";""" % (rtoday, statis_day), stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if p.returncode != 0:
        return None
    m = pd.read_csv(StringIO(out), sep='\t', comment=None)
    h = m[m['status']==1][['from', 'userid']].groupby('from').apply(lambda x: len(x['userid'].unique()))
    web_user_count, android_user_count, ios_user_count = (h.get(0, 0), h.get(1,0), h.get(2,0))
    h = m[m['status']==1]['from'].value_counts()
    web_count, android_count, ios_count = (h.get(0, 0), h.get(1,0), h.get(2,0))
    web_android, web_ios = len(set(m[m['from']==0]['userid']).intersection(set(m[m['from']==1]['userid']))), len(set(m[m['from']==0]['userid']).intersection(set(m[m['from']==2]['userid'])))
    with_price, with_content, with_shop = len(m[(m['price']!=0.0)&(m['status']==1)]), m[m['status']==1]['content'].count(), len(m[(m['shopid']!=0)&(m['status']==1)])
    verified, disverified = len(m[m['status']==1]), len(m[m['status']==0])
    return verified, disverified, web_count, web_user_count, android_count, android_user_count, ios_count, ios_user_count, web_android, web_ios, with_price, with_content, with_shop


def main():
    cur = preday7_date
    while cur != poststatis_date:
        ret = rpt_qnc_paishare_dm(cur)
        if ret:
            if not os.path.exists('/tmp/lifangxing'):
                os.mkdir('/tmp/lifangxing')
            with open('/tmp/lifangxing/lfx_paishare', 'w') as fw:
                fw.write('\t'.join([ str(i) for i in ret ]) + '\n')
            p = subprocess.Popen('''hive -e "load data local inpath '/tmp/lifangxing/lfx_paishare' overwrite into table bing.rpt_qnc_paishare_dm partition (ptdate=%s)"'''%datetime.strptime(cur, '%Y-%m-%d').strftime("%Y%m%d"), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out, err = p.communicate()
            if p.returncode != 0:
                print 'load data %s Failed' % cur
                print err
            print '%s OK' % cur
            os.remove('/tmp/lifangxing/lfx_paishare')
        else:
            print 'select %s Failed' % cur

        cur = (datetime.strptime(cur, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    sys.exit(0)

def recover():
    cur = '2014-09-01'
    f = open('recover.csv', 'w')
    while cur != today:
        ret = rpt_qnc_paishare_dm(cur)
        if ret:
            f.write('\t'.join([ str(i) for i in ret ]) + '\t%s' % datetime.strptime(cur, '%Y-%m-%d').strftime('%Y%m%d') + '\n')
            f.flush()
            print '%s OK' % cur
            print '\t'.join([ str(i) for i in ret ]) + '\t%s' % datetime.strptime(cur, '%Y-%m-%d').strftime('%Y%m%d') + '\n'
        else:
            print '%s Failed' % cur
        cur = (datetime.strptime(cur, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    f.close()
 
main()
