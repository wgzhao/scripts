#encoding=utf-8

import sys
import os

sys.path.append("../log")
import searchKeyword45
import clickCount
import cardClick
sys.path.append("../util")
import DictUtil

def count():
	ts={}
	for line in open("/home/zhangzhonghui/data/backup.1203/tagQuery.all.txt"):
		ts["v45_"+line.strip()]=1
	dir=sys.argv[1]
	files=os.listdir(dir)
	files=sorted(files)
	ccs={}
	for file in files:
		if not file.startswith("searchKeyword45.2014-12-"):
			continue
		for line in open(dir+"/"+file):
			p=line.find("\t")
			k=line[0:p]
			if k not in ts:
				continue
			cols=line.strip().split("\t")
			try:
				k,cc=clickCount.readClick(cols)
			except:
				continue
			if k in ccs:
				ccs[k].merge(cc)
			else:
				ccs[k]=cc
	whr={}
	for k in ccs:
		whr[k]=ccs[k].weitHitRate()
	num=0
	for w in whr:
		if num > 10:
			break
		print w,whr[w]
		num+=1
	ccs={}
	for file in files:
		if not file.startswith("searchKeyword45.2014-12-"):
			continue
		f=open(dir+"/"+file)
		for line in f:
			p=line.find("\t")
			k=line[0:p]
			if not k.startswith(searchKeyword45.CardFix):
				continue
			cols=line.strip().split("\t")
			cc=cardClick.readCardClick(cols)
			k=k[len(searchKeyword45.CardFix):]
			if k not in ccs:
				ccs[k]=cc
			else:
				ccs[k].merge(cc)
		f.close()
	if True:
		for k in ccs:
			cc=ccs[k]
			sumWeit=0
			ksum=clickCount.dictSum(cc.keywordShow)
			for w in cc.keywordShow:
				if w == clickCount.SumMark:
					continue
				wh=0
				if w in whr:
					wh=whr[w]
				sumWeit+=wh*(cc.keywordShow[w]/float(ksum))
			#print k,ksum,sumWeit
			if clickCount.SumMark in cc.ridShow:
				del cc.ridShow[clickCount.SumMark]
			if clickCount.SumMark in cc.ridHit:
				del cc.ridHit[clickCount.SumMark]
			if clickCount.SumMark in cc.keywordShow:
				del cc.keywordShow[clickCount.SumMark]
			print "%s\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%s\t%s\t%s"%(k,cc.showNum(),cc.hitNum(),cc.hitRate(),cc.weitHitRate(),sumWeit,cc.weitHitRate()/(sumWeit+1e-32),clickCount.topnStr(cc.keywordShow),clickCount.topnStr(cc.ridShow),clickCount.topnStr(cc.ridHit))

def getW(w):
	if w.startswith("##Card##_v44_"):
		return (w[13:],1)
	elif w.startswith("##Card##_"):
		return (w[9:],2)
	return (w,0)


class CardRet(object):
	def __init__(self,w):
		self.w=w
		self.s5=0
		self.t5=0
		self.s4=0
		self.t4=0

	def addCardClick(self,cc):
		(w,type)=getW(cc.card)
		if w != self.w:
			return
		sn=cc.showNum()
		tn=cc.hitNum()
		if type == 1:
			self.s4+=sn
			self.t4+=tn
		elif type == 2:
			self.s5+=sn
			self.t5+=tn
	
	def addCount(self,type,sn,tn):
		if type == 1:
			self.s4+=sn
			self.t4+=tn
		elif type == 2:
			self.s5+=sn
			self.t5+=tn

	def __str__(self):
		r4=(self.t4+1.0)/(self.s4+10.0)
		r5=(self.t5+1.0)/(self.s5+10.0)
		return "%s\t%d\t%d\t%.3f\t%d\t%d\t%.3f\t%.3f"%(self.w,self.s4,self.t4,r4,self.s5,self.t5,r5,r5/(r4))

def newCount():
	qcr={}
	for line in sys.stdin:
		if not line.startswith("##Card##_"):
			continue
		cols=line.strip().split("\t")
		tcc=cardClick.readCardClick(cols)
		(w,type)=getW(tcc.card)
		for query in tcc.keywordShow:
			q=query.strip()
			if q not in qcr:
				qcr[q]={}
			cr=qcr[q]
			if w not in cr:
				cr[w]=CardRet(w)
			sn=tcc.keywordShow[query]
			tn=0
			if query in tcc.keywordHit:
				tn=tcc.keywordHit[query]
			cr[w].addCount(type,sn,tn)
		for pos in tcc.posShow:
			q="p_"+str(pos)
			if q not in qcr:
				qcr[q]={}
			cr=qcr[q]
			w="pos"
			if w not in cr:
				cr[w]=CardRet(w)
			sn=tcc.posShow[pos]
			tn=0
			if pos in tcc.posHit:
				tn=tcc.posHit[pos]
			cr[w].addCount(type,sn,tn)

	for query in qcr:
		for card in qcr[query]:
			print "%s\t%s"%(query,str(qcr[query][card]))

if __name__ == "__main__":
	#count()
	newCount()

