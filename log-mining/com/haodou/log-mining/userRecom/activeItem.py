#encoding=utf-8

import sys
sys.path.append("./")
from userRecomConf import *

from datetime import date,datetime,timedelta

def filter(d,n):
	if len(d) < n:
		return d
	sortedItems=sorted(d.items(),key=lambda k:k[1],reverse=True)
	newDict={}
	for k,c in sortedItems:
		if len(newDict) >= n:
			break
		newDict[k]=c
	return newDict

def itemNum(N=7):
	firstDay=datetime.strftime(date.today()-timedelta(days=N),"%Y-%m-%d")
	#print firstDay
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if DaysKey == cols[2]:
			n=0
			i=4
			while i < len(cols):
				day=cols[i]
				i+=1
				if day >= firstDay:
					n+=int(cols[i])
				i+=1
			if n > 0:
				print cols[0]+"\t"+cols[1]+"\t"+cols[3]+"\t"+str(n)
			else:
				pass
				#sys.stderr.write(line)

def filterItem():
	ets={}
	users={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if cols[1] == EntityUser:
			entity=cols[0]
			entityType=getEntityType(entity)
			if entityType not in ets:
				ets[entityType]={}
			if entity not in ets[entityType]:
				ets[entityType][entity]=int(cols[3])
			else:
				ets[entityType][entity]+=int(cols[3])
		else:
			user=cols[0]
			if user not in users:
				users[user]=int(cols[3])
			else:
				users[user]+=int(cols[3])
	aes={}
	for entityType in ets:
		if entityType == "rid":
			tmp=filter(ets[entityType],ActiveEntityRecipeNum)
		else:
			tmp=filter(ets[entityType],ActiveEntityTypeNum)
		for e,n in tmp.items():
			aes[e]=n
	aus=filter(users,ActiveUserNum)
	for e in aes:
		print "%s\t%s\t%d"%(e,EntityUser,aes[e])
	for u in aus:
		print "%s\t%s\t%d"%(u,UserEntity,aus[u])

if __name__=="__main__":
	if sys.argv[1] == "num":
		itemNum(7)
	elif sys.argv[1] == "filter":
		filterItem()



