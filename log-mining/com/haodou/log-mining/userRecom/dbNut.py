#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../util")
sys.path.append("../userRecom")

import hdfsFile
import TimeUtil

from DBUtil2 import *
from nut import *
from userRecomConf import *
import time
from datetime import datetime,date,timedelta

def readComment(day,end):
	db=DB()
	db.connect("haodou_comment")
	#cursor=db.execute("desc Comment;")
	cursor=db.execute("select UserId,ItemId,Type,CreateTime,content from Comment where CreateTime >='"+day+" 00:00:00' and CreateTime <= '"+end+" 23:59:59' and Status = 1")
	ret=cursor.fetchall()
	for r in ret:
		if r[0] == None: #用户名为None忽略,为第三方登陆用户
			continue
		entityFix="rid-"
		if r[2] == 0:
			entityFix="rid-"
		elif r[2] == 1:
			entityFix="aid-"
		elif r[2] == 6:
			entityFix="tid-"
		else:
			entityFix="t"+str(r[2])+"-" #@TODO,最近有类型12和15没有识别
			continue
		nutPrint("uid-"+str(r[0]),"comment",entityFix+str(r[1]),str(int(time.mktime(r[3].timetuple()))))


def readMall(day,end):
	db=DB()
	db.connect("haodou_mall")
	cursor=db.execute("select og.UserId,og.GoodsId,max(ol.CreateTime),ol.OrderId from OrderBase as ob,OrderGoods as og, OrderLog as ol where og.OrderId=ob.OrderId and ob.OrderId=ol.OrderId and ob.OrderStatus=70 and ol.CreateTime >='"+day+" 00:00:00' and ol.CreateTime <= '"+end+" 23:59:59' group by og.UserId,og.GoodsId,ol.OrderId;")
	ret=cursor.fetchall()
	for r in ret:
		#sys.stdout.write(str(r[2])+"\n")
		nutPrint("uid-"+str(r[0]),"buy","goods-%d"%(r[1]),str(int(time.mktime(r[2].timetuple()))))

if __name__=="__main__":
	if len(sys.argv) >= 4 and sys.argv[1] == "acc":
		hdfsDir=sys.argv[2]
		N=int(sys.argv[3])
		sortedDays=hdfsFile.getDays(hdfsDir)
		if len(sortedDays) > 0:
			lastDay=TimeUtil.addDay(sortedDays[-1],1)
		else:
			lastDay=datetime.strftime(date.today()-timedelta(days=N),"%Y-%m-%d")
		end=datetime.strftime(date.today()-timedelta(days=1),"%Y-%m-%d")
		sys.stderr.write("lastDay for dbNut:"+lastDay+"\n")
		sys.stderr.write("end for dbNut:"+end+"\n")
		readComment(lastDay,end)
		readMall(lastDay,end)
	elif len(sys.argv) >= 3:
		readComment(sys.argv[1],sys.argv[2])
		readMall(sys.argv[1],sys.argv[2])
	elif len(sys.argv) >= 2:
		N=int(sys.argv[1])
		end=datetime.strftime(date.today()-timedelta(days=1),"%Y-%m-%d")
		day=datetime.strftime(date.today()-timedelta(days=N),"%Y-%m-%d")
		print N,day,end
		readComment(day,end)
		readMall(day,end)
	else:
		end=datetime.strftime(date.today()-timedelta(days=1),"%Y-%m-%d")
		readComment(end,end)
		readMall(end,end)

