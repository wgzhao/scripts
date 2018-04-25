#encoding=utf-8
#

import sys
sys.path.append("./")
sys.path.append("../util")
import DictUtil
from moreSearchCount import *

SumMark="#sum#"
FirstPageNum=20

pweit=[1.0,
0.500893795,
0.336698068,
0.26647845,
0.233578299,
0.193421389,
0.156896405,
0.139595097,
0.130671339,
0.123464234
]

MaxTitleLen=33
MaxTitleUnicodeLen=11
V45Fix="v45_"

hitTitleTypeName={
	"e":"相同",
	"w":"包含",
	"n":"不包含",
	"p":"包含未显示"
	}

def hitTitle(title,keyword):
	kw=keyword
	if keyword.startswith(V45Fix):
		kw=keyword[4:]
	if kw == title:
		return "e"
	try:
		kw=kw.decode("utf-8")
		tt=title.decode("utf-8")
	except:
		return "n"
	if len(kw) > MaxTitleUnicodeLen:
		kw=kw[0:MaxTitleUnicodeLen]
	p=tt.find(kw)
	if p >= 0:
		end=p+len(kw)
		if end > MaxTitleUnicodeLen:
			return "p"
		else:
			return "w"
	return "n"

def clickWeit(pos):
	if pos <= 9:
		return pweit[pos]
	if pos <= 3:
		return 1.0/(1.0+pos)
	else:
		return 1.0/(0.5+pos)

def listStr(sd,n=500):
	nn=0
	s="{"
	for w,v in sd:
		if len(s) > 1:
			s+=","
		if type(v) == float:
			s+=str(w)+":%.2f"%(v)
		else:
			s+=str(w)+":"+str(v)
		nn+=1
		if nn >= n:
			break
	s+="}"
	return s

def dictSum(d):
	if SumMark not in d:
		return DictUtil.sum(d)
	return d[SumMark]

def topnStr(d,n=500):
	if len(d) == 0:
		return ""
	sd=sorted(d.items(),key=lambda e:e[1],reverse=True)
	return listStr(sd,n)

def nameTopn(name,d):
	if len(d) == 0:
		return ""
	s=dictSum(d)
	if type(s) == float:
		return "\t"+name+":%.1f\t"%(s)+topnStr(d)
	else:
		return "\t"+name+":%d\t"%(s)+topnStr(d)

def removeSumItem(d):
	if SumMark in d:
		del d[SumMark]
	return d

class Click(object):
	def __init__(self):
		self.search=0
		self.hasHit=0
		self.albumShow={}
		self.albumHit={}
		self.topicShow={}
		self.topicHit={}
		self.posHit={}
		self.posShow={}
		self.hit={}
		self.show={}
		self.weitShow={}
		self.hitCount={}
		self.ms=None
		self.foodShow={}
		self.foodHit={}
		self.hitTitleShow={}
		self.hitTitleHit={}
	
	def addItem(self,item,pos):
		if item not in self.show:
			self.show[item]=0
			self.weitShow[item]=0
		if pos not in self.posShow:
			self.posShow[pos]=0
		self.posShow[pos]+=1
		self.show[item]+=1
		self.weitShow[item]+=clickWeit(pos)

	def addSearch(self,rids,album="",topic="",foodId=""):
		self.search+=1
		for rid in rids:
			self.addItem(rid,rids[rid])
		if album != "":
			if album not in self.albumShow:
				self.albumShow[album]=0
			self.albumShow[album]+=1
		if topic != "":
			if topic not in self.topicShow:
				self.topicShow[topic]=0	
			self.topicShow[topic]+=1
		if foodId != "":
			if foodId not in self.foodShow:
				self.foodShow[foodId]=0
			slef.foodShow[foodId]+=1
	
	def addSearchRet(self,ret,keyword):
		self.search+=1
		if ret == None:
			return
		if "rids" in ret:
			for i in range(len(ret["rids"])):
				if i >= FirstPageNum:
					break
				self.addItem(ret["rids"][i],i)
		if "aid" in ret:
			DictUtil.addOne(self.albumShow,ret["aid"])
		if "topicId" in ret:
			DictUtil.addOne(self.topicShow,ret["topicId"])
		if "food" in ret:
			DictUtil.addOne(self.foodShow,ret["food"])
		if "rtitles" in ret:
			for pos in range(len(ret["rtitles"])):
				if pos >= FirstPageNum:
					break
				title=ret["rtitles"][pos]
				#print title,keyword,hitTitle(title,keyword)
				DictUtil.addOne(self.hitTitleShow,hitTitle(title,keyword)+"%d"%(pos))
		if "atitle" in ret:
			title=ret["atitle"]
			DictUtil.addOne(self.hitTitleShow,hitTitle(title,keyword)+"a")
		if "ttitle" in ret:
			title=ret["ttitle"]
			DictUtil.addOne(self.hitTitleShow,hitTitle(title,keyword)+"t")

	def addRecipeHit(self,rid,pos):
		if rid not in self.hit:
			self.hit[rid]=0
		self.hit[rid]+=1
		if pos not in self.posHit:
			self.posHit[pos]=0
		self.posHit[pos]+=1

	def addAlbumHit(self,album):
		DictUtil.addOne(self.albumHit,album)
	
	def addTopicHit(self,topic):
		DictUtil.addOne(self.topicHit,topic)

	def addFoodHit(self,foodId):
		DictUtil.addOne(self.foodHit,foodId)
	
	def addHitCount(self,hc):
		DictUtil.addOne(self.hitCount,hc)

	def addHitTitleHit(self,title,type,keyword):
		DictUtil.addOne(self.hitTitleHit,hitTitle(title,keyword)+type)

	def addTitleHit(self,title,pos,keyword):
		self.addHitTitleHit(title,"%d"%pos,keyword)

	def addAlbumTitleHit(self,title,keyword):
		self.addHitTitleHit(title,"a",keyword)

	def addTopicTitleHit(self,title,keyword):
		self.addHitTitleHit(title,"t",keyword)

	def addHasHit(self):
		self.hasHit+=1

	def getMs(self):
		if self.ms==None:
			self.ms=MoreSearch()
		return self.ms

	def merge(self,ck):
		self.search+=ck.search
		self.hasHit+=ck.hasHit
		self.weitShow=DictUtil.merge(self.weitShow,ck.weitShow)
		self.posHit=DictUtil.merge(self.posHit,ck.posHit)
		self.hit=DictUtil.merge(self.hit,ck.hit)
		self.show=DictUtil.merge(self.show,ck.show)
		self.posShow=DictUtil.merge(self.posShow,ck.posShow)
		self.albumShow=DictUtil.merge(self.albumShow,ck.albumShow)
		self.albumHit=DictUtil.merge(self.albumHit,ck.albumHit)
		self.topicShow=DictUtil.merge(self.topicShow,ck.topicShow)
		self.topicHit=DictUtil.merge(self.topicHit,ck.topicHit)
		self.foodShow=DictUtil.merge(self.foodShow,ck.foodShow)
		self.foodHit=DictUtil.merge(self.foodHit,ck.foodHit)
		self.hitCount=DictUtil.merge(self.hitCount,ck.hitCount)
		self.hitTitleShow=DictUtil.merge(self.hitTitleShow,ck.hitTitleShow)
		self.hitTitleHit=DictUtil.merge(self.hitTitleHit,ck.hitTitleHit)
		if self.ms == None:
			self.ms=ck.ms
		else:
			self.ms.merge(ck.ms)

	def weitHitRate(self):
		weitShowSum=dictSum(self.weitShow)
		if weitShowSum <= 0:
			return 0
		return dictSum(self.hit)/weitShowSum

	def __str__(self):
		s="%d\t%d"%(self.search,self.hasHit)
		s+=nameTopn("weitShow",self.weitShow)
		s+=nameTopn("posHit",self.posHit)
		s+=nameTopn("hit",self.hit)
		s+=nameTopn("show",self.show)
		s+=nameTopn("posShow",self.posShow)
		s+=nameTopn("albumShow",self.albumShow)
		s+=nameTopn("albumHit",self.albumHit)
		s+=nameTopn("topicShow",self.topicShow)
		s+=nameTopn("topicHit",self.topicHit)
		s+=nameTopn("foodShow",self.foodShow)
		s+=nameTopn("foodHit",self.foodHit)
		s+=nameTopn("hitCount",self.hitCount)
		s+=nameTopn("hitTitleShow",self.hitTitleShow)
		s+=nameTopn("hitTitleHit",self.hitTitleHit)
		if self.ms != None:
			s+="\tmoreSearch\t"+str(self.ms)
		return s

	def printHitTitleHitRate(self):
		print "包含类型\t内容类型\t展示数\t点击数\t点击率"
		hit={}
		for type in self.hitTitleShow:
			if type.startswith("#"):
				continue
			cn=type[0:1]
			if cn in hitTitleTypeName:
				cn=hitTitleTypeName[cn]
			ce=type[1:]
			if ce == "a":
				ce="专辑"
			elif ce == "t":
				ce="话题"
			else:
				ce="菜谱第"+ce+"位"
			sn=self.hitTitleShow[type]
			if cn not in hit:
				hit[cn]=[0,0]
			hit[cn][0]+=sn
			tn=0
			if type in self.hitTitleHit:
				tn=self.hitTitleHit[type]
			hit[cn][1]+=tn
			print "%s\t%s\t%d\t%d\t%.3f"%(cn,ce,sn,tn,float(tn)/sn)
		for cn in hit:
			sn=hit[cn][0]
			tn=hit[cn][1]
			ce="全部"
			print "%s\t%s\t%d\t%d\t%.3f"%(cn,ce,sn,tn,float(tn)/sn)

def readDict(s):
	d={}
	s=s.strip()[1:-1]
	tt=s.split(",")
	for t in tt:
		k,v=t.split(":")
		k=k.strip()
		v=v.strip()
		try:
			k=eval(k)
		except:
			pass
		d[k]=eval(v)
	return d


def readClick(cols):
	ck=Click()
	if True:
		kw=cols[0]
		ck.search=int(cols[1])
		ck.hasHit=int(cols[2])
		for i in range(3,len(cols),2):
			if cols[i] == "moreSearch":
				ck.ms=readMoreSearch(cols[i+1:])
				break
			(name,v)=cols[i].split(":")
			v=eval(v)
			try:
				d=readDict(cols[i+1])
				if SumMark not in d:
					d[SumMark]=v
			except:
				sys.stderr.write(line)
				d=cols[i]
			#print type(d)
			setattr(ck,name,d)
	return (kw,ck)

def readAttribute(line,name):
	p=line.find(name+":")
	if p < 0:
		return None
	s=line.find("\t",p)+1
	e=line.find("\t",s)
	return readDict(line[s:e].strip())


def testClick():
	ck=Click()
	rids={123:0,223:1,239:2}
	ck.addSearch(rids,23)
	ck.addRecipeHit(123,0)
	ck.addAlbumHit(23)
	print ck
	for line in open("/home/zhangzhonghui/data/backup.1203/searchKeyword45.2014-11-25"):
		cols=line.strip().split("\t")
		print "read show:",readAttribute(line,"show")
		keyword,tck=readClick(cols)
		print keyword,tck
		ck.merge(tck)
		print ck
		print ck.weitHitRate()
		break
	for i in range(10):
		print clickWeit(i)

def testHitTitle():
	title="豆腐汤"
	keyword="豆腐汤"
	print title,keyword,hitTitle(title,keyword)
	print hitTitle("煮豆腐汤",keyword)
	print hitTitle("豆腐",keyword)
	print hitTitle("哈哈",keyword)

if __name__=="__main__":
	testHitTitle()
	testClick()

