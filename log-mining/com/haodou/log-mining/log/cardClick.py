#encoding=utf-8

import sys
sys.path.append("./")
from clickCount import *
sys.path.append("../util")
import DictUtil

class CardClick(object):
	def __init__(self,card):
		self.card=card
		self.posShow={}
		self.posHit={}
		self.keywordShow={}
		self.keywordHit={}
		self.ridShow={}
		self.ridHit={}

	def showNum(self):
		return dictSum(self.posShow)
	
	def hitNum(self):
		return dictSum(self.posHit)
	
	def hitRate(self):
		if self.showNum() == 0:
			return 0
		return float(self.hitNum())/self.showNum()
		
	def weitHitRate(self):
		weit=0
		for pos in self.posShow:
			if pos == SumMark:
				continue
			weit+=clickWeit(pos)*self.posShow[pos]
		if weit <= 0:
			return 0
		hitNum=self.hitNum()
		return float(hitNum)/weit
	
	def addHit(self,keyword,rid,pos):
		DictUtil.addOne(self.keywordHit,keyword)
		DictUtil.addOne(self.posHit,pos)
		DictUtil.addOne(self.ridHit,rid)
	
	def addSearch(self,keyword,rid,pos):
		DictUtil.addOne(self.keywordShow,keyword)
		DictUtil.addOne(self.posShow,pos)
		DictUtil.addOne(self.ridShow,rid)
	
	def merge(self,cc):
		self.posShow=DictUtil.merge(self.posShow,cc.posShow)
		self.posHit=DictUtil.merge(self.posHit,cc.posHit)
		self.keywordHit=DictUtil.merge(self.keywordHit,cc.keywordHit)
		self.keywordShow=DictUtil.merge(self.keywordShow,cc.keywordShow)
		self.ridShow=DictUtil.merge(self.ridShow,cc.ridShow)
		self.ridHit=DictUtil.merge(self.ridHit,cc.ridHit)

	def __str__(self):
		s=self.card
		s+=nameTopn("posShow",self.posShow)
		s+=nameTopn("posHit",self.posHit)
		s+=nameTopn("keywordShow",self.keywordShow)
		s+=nameTopn("keywordHit",self.keywordHit)
		s+=nameTopn("ridShow",self.ridShow)
		s+=nameTopn("ridHit",self.ridHit)
		return s

class CardBank(object):
	def __init__(self):
		self.bank={}
		self.reset()

	def reset(self):
		self.ridCard={}
		self.rids={}

	def addSearch(self,keyword,ret,v45=True):
		self.reset()
		if "rids" in ret:
			for i in range(len(ret["rids"])):
				rid=ret["rids"][i]
				self.rids[rid]=i
		if "card" in ret:
			self.ridCard=ret["card"]
			for rid in ret["card"]:
				card=ret["card"][rid]
				if not v45:
					card="v44_"+card
				if card not in self.bank:
					self.bank[card]=CardClick(card)
				pos=self.rids[rid]
				self.bank[card].addSearch(keyword,rid,pos)

	def addHit(self,keyword,rid,pos,v45=True):
		if rid in self.ridCard:
			card=self.ridCard[rid]
			if not v45:
				card="v44_"+card
			pos=self.rids[rid]
			if card not in self.bank:
				return
			self.bank[card].addHit(keyword,rid,pos)

def readCardClick(cols):
	card=cols[0]
	cc=CardClick(card)
	for i in range(1,len(cols),2):
		(name,v)=cols[i].split(":")
		v=eval(v)
		try:
			d=readDict(cols[i+1])
			if SumMark not in d:
				d[SumMark]=v
		except:
			d=cols[i+1]
			sys.stderr.write(line)
		setattr(cc,name,d)
	return cc

#
# hdfs dfs -text /user/zhangzhonghui/logcount/userSort/2014-12-02/part-00001.lzo | python cardClick.py | more
def test():
	cc=CardClick("人气")
	print type(cc)
	cc.posHit[0]=1
	print cc
	sys.path.append("../")
	import column
	sys.path.append("../abtest/")
	import column2
	cardBank=CardBank()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS+1:
			continue
		method=cols[column.METHOD_CID+1]  #获得请求的方法
		if method == "search.getlist" or method == "search.getsearchindex":
			keyword=column.getValue(cols[column.PARA_ID+1],"keyword")
			scene=column.getValue(cols[column.PARA_ID+1],"scene")
			tagid=column.getValue(cols[column.PARA_ID+1],"tagid")
			version=cols[column.VERSION_CID+1]
			ip=cols[0]
			if version == "":
				continue
			ret=column2.FuncMap[method](cols[-1])   #获得搜索返回的食谱列表
			if (scene == "" or scene == "k1") and tagid == "" and keyword == "" and "food" in ret and ret["food"] != None and ret["food"] != 0:
				print ret["food"]
				print ip
				#print line
				continue
			cardBank.addSearch(keyword,ret)
	#for card in cardBank.bank:
	#	print card,cardBank.bank[card]

if __name__ == "__main__":
	test()

