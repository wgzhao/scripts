#! /usr/bin/env python
# -*- coding: utf-8 -*-

from optparse import OptionParser
import logging 
import os
import re
import datetime

logging.basicConfig(filename='dw_app_actionlog_visit_dm.log',level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

parser = OptionParser()  
parser.add_option("-d", "--date", dest="date", help="log date, format like 2014-06-06", metavar="DATE")

(options, args) = parser.parse_args()
today = datetime.date.today()
options.date = options.date or today.strftime('%Y-%m-%d')


logging.info("Running ...")
logging.info("Params:" + str(options))
                         
cmd = "hive -S  "
sql = '''
select get_json_object(json_msg,'$.b.e') as app_id,
get_json_object(json_msg,'$.b.d') as version_id,
get_json_object(json_msg,'$.b.a') as dev_uuid,
case when get_json_object(json_msg,'$.ext.action') in ('A1000','A1002') then get_json_object(json_msg,'$.ext.page') end as pagecode,
get_json_object(json_msg,'$.device_time') as visit_time0,
get_json_object(json_msg,'$.server_time') as visit_time1,
coalesce(get_json_object(json_msg,'$.device_time'),get_json_object(json_msg,'$.server_time')) as visit_time
from bing.ods_app_actionlog_raw_dm
where statis_date='%(date)s' order by visit_time ''' % {'date':options.date}

cmd = "%(cmd)s -e \"%(sql)s\" 2>/dev/null" % {'cmd':cmd, 'sql':sql}
logging.info(cmd)
out = os.popen(cmd)

e_app_id, e_version_id, e_dev_uuid, e_pagecode, e_device_time, e_server_time, e_visit_time = range(7)
r_app_id,r_dev_uuid,r_version_id,r_session_id,r_seq,r_pagecode,r_refcode,r_visit_time,r_visit_dur = range(9)


data = {}
session_id = 0
while True:
  line = out.readline()
  if not line:
    break
  row = re.sub("\n$", "", line)
  row = row.split("\t")
  if len(row)< 5:
    continue
  key = row[e_app_id] + row[e_version_id] + row[e_dev_uuid]

  if key not in data:
    if row[e_pagecode] == 'NULL':
      continue
    session_id += 1
    data[key] = {}
    data[key]['last'] = row[e_visit_time]
    data[key]['start'] = row[e_visit_time]
    data[key]['seq'] = 1
    data[key]['last_code'] = row[e_pagecode]
    data_row = {}
    data_row['r_app_id'] = row[e_app_id]
    data_row['r_dev_uuid'] = row[e_dev_uuid]
    data_row['r_version_id'] = row[e_version_id]
    data_row['r_session_id'] = session_id
    data_row['r_seq'] = data[key]['seq']
    data_row['r_pagecode'] = row[e_pagecode]
    data_row['r_refcode'] = '^'
    data_row['r_visit_time'] = row[e_visit_time]
    data_row['r_visit_dur'] = 0
    data[key]['row'] = []
    data[key]['row'].append(data_row)
  else:
    if int(row[e_visit_time]) - int(data[key]['last']) < 60 * 5:
      data[key]['last'] = row[e_visit_time]
      if row[e_pagecode] != 'NULL' and row[e_pagecode] != data[key]['last_code']:
        data[key]['seq'] += 1
        data_row = {}
        data_row['r_app_id'] = row[e_app_id]
        data_row['r_dev_uuid'] = row[e_dev_uuid]
        data_row['r_version_id'] = row[e_version_id]
        data_row['r_session_id'] = data[key]['row'][-1]['r_session_id']
        data_row['r_seq'] = data[key]['seq']
        data_row['r_pagecode'] = row[e_pagecode]
        data_row['r_refcode'] = data[key]['last_code']
        data[key]['last_code'] = row[e_pagecode]
        data_row['r_visit_time'] = row[e_visit_time]
        data_row['r_visit_dur'] = 0
        data[key]['row'][-1]['r_visit_dur'] = int(row[e_visit_time]) - int(data[key]['row'][-1]['r_visit_time'])
        data[key]['row'].append(data_row)
      else:
        data[key]['last'] = row[e_visit_time]
    else:
      if row[e_pagecode] == 'NULL':
        continue
      session_id += 1
      data[key] = {}
      data[key]['last'] = row[e_visit_time]
      data[key]['start'] = row[e_visit_time]
      data[key]['seq'] = 1
      data[key]['last_code'] = row[e_pagecode]
      data_row = {}
      data_row['r_app_id'] = row[e_app_id]
      data_row['r_dev_uuid'] = row[e_dev_uuid]
      data_row['r_version_id'] = row[e_version_id]
      data_row['r_session_id'] = session_id
      data_row['r_seq'] = data[key]['seq']
      data_row['r_pagecode'] = row[e_pagecode]
      data_row['r_refcode'] = '^'
      data_row['r_visit_time'] = row[e_visit_time]
      data_row['r_visit_dur'] = 0
      data[key]['row'].append(data_row)

a_file_row = ''
a_file_row += "%(app_id)s" + "\001"
a_file_row += "%(dev_uuid)s" + "\001"
a_file_row += "%(version_id)s" + "\001"
a_file_row += "%(session_id)s" + "\001"
a_file_row += "%(seq)s" + "\001"
a_file_row += "%(pagecode)s" + "\001"
a_file_row += "%(refcode)s" + "\001"
a_file_row += "%(visit_time)s" + "\001"
a_file_row += "%(visit_dur)s" + "\n"

tmp_file = "dw_app_actionlog_visit_dm_%(date)s.dat" % {'date': options.date}
logging.info("Start to write file:" + tmp_file)
dat = open(tmp_file, 'w+')
row_cnt = 0
for i in data:
  for r in data[i]['row']:
    a_row = a_file_row % {'app_id':r['r_app_id'], 'dev_uuid': r['r_dev_uuid'], 'version_id': r['r_version_id'], 'session_id': r['r_session_id'], 'seq': r['r_seq'], 'pagecode': r['r_pagecode'], 'refcode': r['r_refcode'], 'visit_time':r['r_visit_time'], 'visit_dur': r['r_visit_dur']}
    dat.write(a_row)
    row_cnt += 1
dat.close()
logging.info("Wrote %(row_cnt)s lines" % {'row_cnt':row_cnt})
logging.info("Closed file:" + tmp_file)

cmd = "hive -S  -e \"load data local inpath '%(file)s' overwrite into table bing.dw_app_actionlog_visit_dm partition (statis_date ='%(date)s')\"" % {'date':str(options.date), 'file':tmp_file}
print cmd
os.popen(cmd)
logging.info(cmd)
#os.popen("rm -rf " + tmp_file)
#logging.info("Remove file:" + tmp_file)
logging.info("Bye ...")








