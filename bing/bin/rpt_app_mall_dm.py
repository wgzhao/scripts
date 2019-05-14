#! /usr/local/bin/python2.7
# encoding: utf-8
import pandas as pd
from StringIO import StringIO
from user_agents import parse
import re, subprocess, sys, md5
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser
from sqlalchemy import *

usageinfo = "%prog [--date=statisdate]"
today = datetime.today().strftime("%Y-%m-%d")
rtoday = datetime.today().strftime("%Y%m%d")

parser = OptionParser(usage=usageinfo, version="%prog 1.0")
parser.set_defaults(statisdate=(datetime.strptime(today,"%Y-%m-%d")+timedelta(days=-1)).strftime("%Y-%m-%d"))
parser.add_option("-d", "--date", dest="statisdate", help="statis date, yyyy-mm-dd or yyyymmdd", metavar="DATE")
sqlengine = create_engine('mysql://ruby:ruby@10.0.11.50:3306/dw?charset=utf8')

(options, args) = parser.parse_args()
statis_date = options.statisdate

sql = "select logdate, http_user_agent, remote_addr, path from logs.m_haodou_com where logdate = '%s' and path like '%%native/mall%%' " % statis_date

p = subprocess.Popen('hive -e "%s"' % sql, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
out, err = p.communicate()

m = pd.read_csv(StringIO(out), header=None, sep='\t', names=['d', 'ua', 'ip', 'path'])
m['os'] = m.ua.apply(lambda x: parse(x).os[0] ).copy()
m = m[ m['os'].str.contains('Android') | m.os.str.contains('iOS') ]

dbuuids = pd.read_sql('select distinct uuid from dw.mall_new_device', sqlengine)
uuidm = pd.DataFrame({'uuid': (m.ua+m.ip).apply(lambda x: md5.new(x).hexdigest()).drop_duplicates() })
uuidm['createtime'] = statis_date
uuidm['isnew'] = 1
uuidm.isnew[ uuidm.uuid.isin(dbuuids.uuid) ]=0
uuidm.to_sql('mall_new_device', con = sqlengine, if_exists='append', index=False)


def path_count(path_regx, extract_pattern = None):
    total = m[ m['path'].str.contains(path_regx) ]
    if extract_pattern is None:
        return total.groupby(['d', 'os']).apply(lambda x: pd.Series({'pv': len(x), 'uv': len((x['ua']+x['ip']).drop_duplicates()), 'ip': len(x['ip'].drop_duplicates()) }) ).reset_index()
    total['id'] = total['path'].apply(lambda x: extract_pattern.findall(x)[0]).copy()
    return total.groupby(['d', 'os', 'id']).apply(lambda x: pd.Series({'pv': len(x), 'uv': len((x['ua']+x['ip']).drop_duplicates()), 'ip': len(x['ip'].drop_duplicates()) })).reset_index()

r1 = path_count('native/mall/index.php') #商城首页
r1['t'] = 1
r2 = path_count('goods.php\?do=ajaxGetData') # 秒杀列表
r2['t'] = 2
r3 = path_count('commodity.php\?do=Special') # 特卖列表
r3['t'] = 3
r4 = path_count('order.php\?do=orderCenter') # 我的订单
r4['t'] = 4
r5 = path_count('goods.php\?do=view') # 限时秒商品页总汇
r5['t'] = 5
r6 = path_count('goods.php\?do=viewImg') # 限时秒商品图文详情总汇
r6['t'] = 6
r7 = path_count('commodity.php\?do=View&_id=[0-9]+?&_v=sp') # 特卖商品页总汇
r7['t'] = 7
r8 = path_count('commodity.php\?do=View&_id=[0-9]+?&_v=dt') # 特卖商品图文详情总汇
r8['t'] = 8
ret = pd.concat([r1, r2 , r3, r4, r5, r6, r7, r8], ignore_index=True)
ret = ret[['os', 'ip', 'pv', 'uv', 't']]
fpath = '/tmp/malllist_%s'%statis_date
ret.to_csv(fpath, header=False, index=False, sep='\t')
p = subprocess.Popen("""hive -e "LOAD DATA local INPATH '%s' OVERWRITE INTO TABLE bing.rpt_app_malllist_dm PARTITION (ptdate='%s')" """ % (fpath, statis_date), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
out, err = p.communicate()
print err

r1=path_count('goods.php\?do=view', re.compile('goods_id=([0-9+])')) # 限时秒
r1['t'] = 1
r2=path_count('goods.php\?do=viewImg', re.compile('goods_id=([0-9+])')) # 限时秒详情
r2['t'] = 2
r3=path_count('commodity.php\?do=View&_id=[0-9]+?&_v=sp', re.compile('_id=([0-9]+)')) # 特卖
r3['t'] = 3
r4=path_count('commodity.php\?do=View&_id=[0-9]+?&_v=dt', re.compile('_id=([0-9]+)')) # 特卖详情
r4['t'] = 4
ret = pd.concat([r1, r2 , r3, r4], ignore_index=True)
ret = ret[['os', 'id', 'ip', 'pv', 'uv', 't']]
fpath = '/tmp/mallid_%s'%statis_date
ret.to_csv(fpath, header=False, index=False, sep='\t')
p = subprocess.Popen("""hive -e "LOAD DATA local INPATH '%s' OVERWRITE INTO TABLE bing.rpt_app_mallid_dm PARTITION (ptdate='%s')" """ % (fpath, statis_date), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
out, err = p.communicate()

sys.exit(0)
