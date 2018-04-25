#!/usr/bin/python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import urllib2, json, os 
import time as timer
import requests
from datetime import *

print sys.argv
if len(sys.argv)>1:
  try:
    day = datetime.strptime(sys.argv[1], "%Y-%m-%d")
  except:
    day = datetime.today() - timedelta(days=1)
else:
  day = datetime.today() - timedelta(days=1)

of_name = '/tmp/push/ip/ip_ok.%s.txt' % day.strftime("%Y%m%d")
if_name = '/tmp/push/ip/unknown_ips.%s.txt' % day.strftime("%Y%m%d")

num = 0

def hive_query(sql):
  hive = "/usr/bin/hive --hiveconf mapreduce.map.memory.mb=16384 --hiveconf mapreduce.reduce.memory.mb=8192 -S -e \"add jar /usr/lib/hive/lib/hive-json-serde.jar;" + sql + "\"";
  print hive
  p = os.popen(hive)
  return p.read()

def store(ips, results):
  global outfile 
  global num 
  fmt = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
  buf = ""
  if results.has_key('province') or len(ips)==1:
    info = results
    buf += fmt % (str(ips[0]), str(info['country']), str(info['province']), str(info['city']), str(info['district']), str(info['isp']), str(info['type']), str(info['start']), str(info['end']))
    num += 1
  else:
    for ip in results:
      info = results[ip]
      num += 1
      buf += fmt % (str(ip), str(info['country']), str(info['province']), str(info['city']), str(info['district']), str(info['isp']), str(info['type']), str(info['start']), str(info['end']))

  outfile.write(buf)
  outfile.flush()

def query_from_baidu(ipstr):
    base_url = 'https://api.map.baidu.com/location/ip'
    payload={'ak':'eW105MKyumrKpIqWRltLugM33QupkupQ','ip':ipstr,'coor':'bd09ll'}
    r = requests.get(base_url,params=payload)
    if r.status_code == 200:
        print r.json()
    else:
        print r.json()
        
def query_from_sina():
  infile = open(if_name, 'r')

  base_url='http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip='

  url = base_url
  
  iplist = []
  for ip in infile.readlines():
    iplist.append(ip[:-1])
    url += ip[:-1] + ","
    if len(url) > 10:
      try:
        resp = urllib2.urlopen(url[:-1])
        buf = resp.read()
        store(iplist, json.loads(buf))
      except Exception as e:
        print(url,  str(e))
        url = base_url
        iplist = []
        continue
      #timer.sleep(0.01)
      url = base_url
      iplist = []

  infile.close()

def query_from_hive():
  #sql = "select distinct(a.ip) from logs.pushlogs a left join common.ip_city b on a.ip = b.ip where a.statis_date='%s' and b.ip is null;" % day.strftime("%Y-%m-%d")
  sql = "select distinct(a.ip) from (select distinct(ip) as ip from logs.pushlogs where statis_date='%s') a left join common.ip_city b on a.ip = b.ip where b.ip is null;" % day.strftime("%Y-%m-%d")
  res = hive_query(sql)
  f = open(if_name, 'w')
  f.write(res)
  f.close()
  print('get %d unknown ips from hive' % len(res.split('\n')))

def load_into_hive():
  sql = "LOAD DATA LOCAL INPATH '%s' INTO TABLE common.ip_city;" % of_name
  res = hive_query(sql)
  print('load_into_hive return', res)


if __name__ == '__main__':
    query_from_baidu('115.28.10.15')
    # query_from_hive()
    # outfile = open(of_name, "w")
    # query_from_sina()
    # print('get %d ip addrs from sina' % num)
    # load_into_hive()

outfile.close()
