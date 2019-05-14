#encoding=utf-8

import sys
sys.path.append("../util")
from DBUtil2 import *

def readUserPhone():
	db=DB()
	#db.connect("recipe_user")
	#db.connect("qunachi_user")
	#db.connect("haodou_connect_recipe")
	#cursor=db.execute("select UserId,Phone from UserInfo")
	#cursor=db.execute("select * from User where UserId = '3274295'")
	#cursor=db.execute("show tables;")
	#cursor=db.execute("select UserId,Mobile,RegTime from User")
	#cursor=db.execute("desc Connect")
	#cursor=db.execute("select * from User limit 1000000,10")
	#cursor=db.execute("select UserId,OpenId,UpdatedAt from Connect where OpenId != ''")
	#cursor=db.execute("select UserId from User")
	cursor=db.execute("show databases;")
	ret=cursor.fetchall()
	#sys.stderr.write("size:%d\n"%(len(ret)))
	for r in ret:
		#for i in range(len(r)):
		#print r[i]
		#if r[1] != None and len(r[1]) > 0:
		#	print "%d\t%s"%(r[0],r[1].encode("utf-8"))
		s=""
		for i in range(len(r)):
			if i > 0:
				s+="\t"	
			if r[i] != None:
				if type(r[i]) == unicode:
					s+=str(r[i].encode("utf-8"))
				else:
					s+=str(r[i])
		print s
	db.close()

if __name__=="__main__":
	readUserPhone()


