#encoding=utf-8

import sys

sys.path.append("./")

def read():
	tags={}
	nn=0
	for line in open("./tag.wap.txt"):
		if nn == 0:
			nn+=1
			continue
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		if len(cols) < 5:
			continue
		tid=cols[0]
		name=cols[1]
		card=cols[2]
		topic=cols[3]
		item=cols[4]
		#http://m.haodou.com/app/recipe/act/tag/index.php?tid=66&sub=two
		if item.startswith("http://m.haodou.com"):
			item=item[len("http://m.haodou.com"):]
		if tid not in tags:
			tags[tid]=(name,{})
		if card not in tags[tid][1]:
			tags[tid][1][card]={}
		if topic not in tags[tid][1][card]:
			tags[tid][1][card][topic]=[]
		if topic.startswith("适宜-") or topic.startswith("禁忌-"):
			if not item.startswith("food-id-"):
				item="food-id-"+item
		tags[tid][1][card][topic].append(item)
	return tags

'''
	tags={}
	lastCard=""
	fix=""
	tid=""
	nn=0
	for line in open("./tag.wap.txt"):
		if nn == 0:
			nn+=1
			continue
		if line.startswith("#"):
			continue
		if len(line.strip()) == 0:
			continue
		cols=line.strip().split("\t")
		if len(cols) < 1:
			continue
		if len(cols) == 5:
			name=cols[0].strip()
			url=cols[4].strip()
			p=url.find("?tid=")
			tid=url[p+len("?tid="):]
			tags[tid]=(name,{})
			card=cols[1].strip()
			tags[tid][1][card]={}
			lastCard=card
			topic=cols[2].strip()
			item=cols[3].strip()
		else:
			card=""
			if len(cols) >= 3:
				card=cols[-3].strip()
			topic=""
			if len(cols) >= 2:
				topic=cols[-2].strip()
			item=cols[-1].strip()
		if card != "" and lastCard != card:
			tags[tid][1][card]={}
			lastCard=card
		if topic != "":
			tags[tid][1][lastCard][topic]=[]
			lastTopic=topic
			p=item.find("-")
			fix=item[0:p+1]
			item=item[p+1:]
		if fix == "tag-":
			fix="keyword-"
		tags[tid][1][lastCard][lastTopic].append(fix+item)
	return tags
'''

def test():
	tags=read()
	for tid in tags:
		(name,cards)=tags[tid]
		for card in cards:
			for topic in cards[card]:
				for item in cards[card][topic]:
					print tid+"\t"+name+"\t"+card+"\t"+topic+"\t"+item

def readNutiWap():
	nutriTags={}
	tag=""
	fix=""
	for line in open("./nutri.wap.txt"):
		if line.startswith("\t"):
			item=line.strip()
			if len(nutriTags[tag]) == 0:
				p=item.find("-")
				fix=item[0:p+1]
				if fix == "topic-":
					fix="tid-"
				item=item[p+1:]
			nutriTags[tag].append(fix+item)
		else:
			tag=line.strip()
			nutriTags[tag]=[]
	return nutriTags

def testN():
	nutriTags=readNutiWap()
	for t in nutriTags:
		items=nutriTags[t]
		for item in items:
			print t,item

if __name__=="__main__":
	test()
	#testN()


