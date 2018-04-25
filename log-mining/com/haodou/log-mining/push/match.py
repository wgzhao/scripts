#encoding=utf-8

import sys
sys.path.append("./")
import itemBank
import idf
import math
import random

targetDate=sys.argv[1]
randomSeed=targetDate.__hash__()

(targetMessage,targetItem,defaultItems,restItems)=itemBank.getItems(targetDate)

sys.path.append("../log")
import queryTag

sys.path.append("../util")
import DictUtil

import weitTag

allItems={}
d=idf.readIdf(3)

for id in restItems:
	item=restItems[id]
	allTags=queryTag.getAllTags(item.title)
	for t in allTags:
		if t in d:
			item.tags[t]=1
	allItems[id]=item

for id in defaultItems:
	item=defaultItems[id][1]
	allTags=queryTag.getAllTags(item.title)
	for t in allTags:
		if t in d:
			item.tags[t]=1
	allItems[id]=item


def userGroup(u):
	s=u.__hash__()
	n=(s+randomSeed)%13
	if n == 0:
		return "random"
	if n == 1:
		return "default"
	if (n >= 3 and n <= 11):  #
		return "reserveDefault"  #不会将进入default推送的直接去掉
	if n == 2:   #这个效果不好，取消，2015-02-05
		return "random_reserveDefault"
	return "weitTag"

sw=1.0
vw=3.0
def output(lastU,uh,utags):
	items=restItems
	g=userGroup(lastU)
	if g.endswith("reserveDefault"):
		items=allItems
	if g == "random" or g == "random_reserveDefault":
		rids=[]
		for id in items:
			if id in uh:
				continue
			rids.append(id)
		if len(rids) > 0:
			rid=random.randrange(len(rids))
			maxId=rids[rid]
			print lastU+"\t"+maxId+"\t"+g
		return
	if g == "default" and targetItem not in uh:
		print lastU+"\t"+targetItem+"\t"+g
		return
	for t in utags:
		utags[t]=math.log(1.0+utags[t])/d[t]
	max=0
	maxId=""
	#if len(utags) > 0:
	#	for tag in utags:
	#		sys.stderr.write(tag+":"+str(utags[t])+",")
	#	sys.stderr.write("\n")
	for id in items:
		if id in uh:
			continue
		s=weitTag.cosineWeit(items[id].tags,utags)
		v=items[id].v*10
		score=math.pow(s+1e-12,sw)*math.pow(v,vw)
		if score > max:
			max=score
			maxId=id
	#if len(utags) > 0:
	#	sys.stderr.write(restItems[maxId].title+"\t"+g+"\n")
	print lastU+"\t"+maxId+"\t"+g

def maxp():
	for line in sys.stdin:
		if line.startswith("deviceid:") or line.startswith("uid:") or line.startswith("haodou"):
			sys.stderr.write(line)
			continue
		if line.startswith("uuid:"):
			line=line[len("uuid:"):]
		print line.strip()

def match():
	lastU=""
	uh={}
	utags={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		u=cols[0]
		p=u.find(":")
		if p >= 0:
			u=u[0:p]
		if lastU == "":
			lastU=u
		if lastU != u:
			output(lastU,uh,utags)
			lastU=u
			uh={}
			utags={}
		if len(cols) ==2: #历史推送
			h=cols[1]
			uh[h]=1
		else: #搜索tag
			for i in range(2,len(cols),1):
				t=cols[i]
				if t not in d:
					continue
				if t not in utags:
					utags[t]=1
				else:
					utags[t]+=1
	if lastU != "":
		output(lastU,uh,utags)

if __name__ == "__main__":
	match()
	#d1={2:3,43:3}
	#d2={2:3}
	#print cosine(d1,d2)

