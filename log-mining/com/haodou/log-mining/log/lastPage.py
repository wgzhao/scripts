#encoding=utf-8
#
#note：为了确定当前方法点击的项目是从什么地方来的，需要确定在前面调用的方法的返回值
#如：为了确定当前点击的话题是不是厨房宝典，需要确定精选首页中有没有这个话题
#那么我们需要确认用户在前一次调用精选首页接口时的返回值是什么
#

import sys

sys.path.append("./")
sys.path.append("../")

from column import *

#topic.getgroupindexdata
#recipe.getfindrecipe
#recipe.getcollectlist
def lastMethod(methods):
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		method=cols[METHOD_CID]
		if method in methods:
			u=ActionUser(cols)
			requestId=getValue(cols[PARA_ID],"request_id")
			if u != None and u != "" and requestId != None and requestId != "":
				mus[method][u]=requestId


