#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../log")
import conf

DefaultV=0.12

def parseUrl(url):
	type=""
	id="0"
	if url.find("album") > 0:
		type="album"
		if url.endswith("/"):
			url=url[0:-1]
		pos=url.rfind("/")	
		id=url[pos+1:]
	elif url.find("topic-") > 0:
		#sys.stderr.write(url+"\n")
		type="topic"
		s=url.find("topic-")
		e=url.find(".",s)
		e1=url.find("&",s)
		if e < 0 or (e1 >  s and e > e1):
			e=e1
		id=url[s:e]
	elif url.find("recipe") > 0:
		type="recipe"
		if url.endswith("/"):
			url=url[0:-1]
		pos=url.rfind("/")
		id="recipe-"+url[pos+1:]
	else:
		#sys.stderr.write(url+"\n")
		pass
	return (type,id)

SpecialMark="_special"

def removeSpecialMark(id):
	p=id.find(SpecialMark)
	if p > 0:
		id=id[0:p]
	return id

class PushItem(object):
	def __init__(self,line):
		self.id=None
		line=line.strip()
		if line.startswith("#"):
			return
		cols=line.strip().split("\t")
		if len(cols) < 2:
			return
		self.title=cols[0]
		self.url=cols[1]
		(type,id)=parseUrl(self.url)
		self.type=type
		self.id=id
		self.tags={}
		self.v=DefaultV
		if len(cols) < 3:
			return
		k=cols[2].split()[0]
		if k.startswith(SpecialMark):
			self.id+=k
		for c in cols[2:]:
			cs=c.split(",")
			for t in cs:
				ccs=t.split("，")
				for tt in ccs:
					self.tags[tt.strip()]=1
		#allTags=queryTag.getAllTags(self.title)
		#for t in allTags:
		#	self.tags[t]=1

	def setV(self,v):
		self.v=v

	def __str__(self):
		if self.id == None:
			return ""
		s="%s\t%s\t%s\t%s\t%f"%(self.id,self.title,self.type,self.url,self.v)
		s+="\t["
		for t in self.tags:
			s+=t+" "
		s+="]"
		return s

def getDefaultItems(targetDate):
	targetId=""
	targetMessage=""
	defaultItems={}
	for line in open("defaultPushItem.txt"):
		line=line.strip()
		if line.startswith("#"):
			continue
		pos=line.find("\t")
		if pos < 0:
			continue
		date=line[0:pos]
		item=PushItem(line[pos+1:])
		if item.id == None:
			continue
		if targetDate == date:
			targetId=item.id
			targetMessage=item.title
		else:
			defaultItems[item.id]=(date,item)
	return (targetMessage,targetId,defaultItems)

def setItemValue(restItems):
	for line in open("itemRate.random.txt"):
		cols=line.strip().split("\t")
		id=cols[0]
		v=float(cols[1])
		#print id,v,cols[1]
		if id in restItems:
			restItems[id].setV(v)
	return restItems

def readItemBank():
	bank={}
	for line in open(conf.itemBankFile):
		item=PushItem(line)
		if item.id == None:
			continue
		bank[item.id]=item
	return bank

def getItems(targetDate):
	restItems={}
	(targetMessage,targetId,defaultItems)=getDefaultItems(targetDate)
	bank=readItemBank()
	for id in bank:
		item=bank[id]
		if item.id != None and item.id not in defaultItems:
			restItems[item.id]=item
	restItems=setItemValue(restItems)
	return (targetMessage,targetId,defaultItems,restItems)

def check(f):
	for line in f:
		item=PushItem(line)
		if item.id != None:
			print item
		else:
			pass
			#print line,None

def testGetItems(date):
	(targetItem,targetId,defaultItems,restItems)=getItems(date)
	print "targetItem:",targetItem
	#print defaultItems
	#print restItems
	for id in restItems:
		pass
		print id,restItems[id]

if __name__=="__main__":
	#check(sys.stdin)
	testGetItems(sys.argv[1])

