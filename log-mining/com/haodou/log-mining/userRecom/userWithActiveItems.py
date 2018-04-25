#encoding=utf-8

import sys

#
#读取流行实体列表
#读取实体的创建用户列表
#
#输出创建出过流行实体的用户列表
#
def findEntityUser():
	activeEntitys={}
	users={}
	for line in open("/home/data/recomData_tmp/item.active.txt"):
		cols=line.strip().split()
		activeEntitys[cols[0]]=int(cols[2])
	for line in sys.stdin:
		cols=line.strip().split()
		entity=cols[0]
		uid=cols[1]
		if entity in activeEntitys:
			if uid not in users:
				users[uid]={}
			users[uid][entity]=activeEntitys[entity]
	for uid in users:
		if len(users[uid]) < 2:
			continue
		s=uid
		n=0
		for e,v in sorted(users[uid].items(),key=lambda k:k[1],reverse=True):
			s+="\t%s\t%d"%(e,v)
			n+=1
			if n >= 100:
				sys.stderr.write("%s\t%r\n"%(uid,len(users[uid])))
				break
		print s

if __name__=="__main__":
	findEntityUser()


