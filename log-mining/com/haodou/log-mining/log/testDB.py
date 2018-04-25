from sharkDB import *

selectStr='''
select 
get_json_object(json_msg,'$.b.e') as app_id,
get_json_object(json_msg,'$.ext.page') as pagecode,
count(1) as pv
from bing.ods_app_actionlog_raw_dm
where statis_date='${statis_date}'
and get_json_object(json_msg,'$.ext.action') in ('A1000','A1002')
'''

selectStr='select * from bing.ods_app_actionlog_raw_dm limit 100'
selectStr="select topicid,cateid from hd_haodou_center_20140811.grouptopic"

db=SharkDB()
def readDBnames():
	db.queryString("show databases;")
	db.queryString("use haodou_recipe_20140501;")
	db.queryString("use bing;")
	ret=db.execute(selectStr)
	#print ret
	for line in ret:
		print line

if __name__=="__main__":
	readDBnames()


