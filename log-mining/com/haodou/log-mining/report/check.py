#encoding=utf-8

import sys
import os

sys.path.append("../log")
import clickCount
import searchKeyword45
sys.path.append("../util")
import DictUtil

def count():
	ts={}
	for line in open("/home/zhangzhonghui/data/backup.1203/tags.txt"):
		ts[line.strip()]=1
		ts["v45_"+line.strip()]=1
	#w=sys.argv[2]
	dir=sys.argv[1]
	files=os.listdir(dir)
	files=sorted(files)
	pn=False	
	for file in files:
		tck=clickCount.Click()
		ack=clickCount.Click()
		if not file.startswith("searchKeyword45.2014-"):
			continue
		for line in open(dir+"/"+file):
			cols=line.strip().split("\t")
			k=cols[0]
			if k.startswith(searchKeyword45.CardFix):
				continue
			#if k.endswith("_##total##"):
			#	(k,click)=clickCount.readClick(cols)
			#	ack.merge(click)
			#	continue
			if (not k.startswith("v45_")) and not k == "ck45_##total##":
				continue
			has=False
			if k in ts:
				has=True
			if False:
				for i in range(len(k)-2):
					if has:
						break
					for j in range(i+2,len(k)+1,1):
						sub=k[i:j]
						if sub in ts:
							has=True
							break
			if not has:
				continue
			(k,click)=clickCount.readClick(cols)
			tck.merge(click)
			#print k
			#voidHit=0
			#if 0 in click.hitCount:
			#	voidHit=click.hitCount[0]
			#hit=DictUtil.sum(click.hit)
			#if clickCount.SumMark in click.hit:
			#	hit=click.hit[clickCount.SumMark]
			#if click.search==0:
			#	click.search+=1e-12
			#print k,click.search
			#print "%s\t%s\t%d\t%d\t%d\t%f\t%f"%(file[-10:],k,click.search,hit,(click.search-voidHit),float(hit)/click.search,float(click.search-voidHit)/click.search)
			#print click
		#hit=clickCount.dictSum(tck.hit)
		#voidHit=0
		#if 0 in tck.hitCount:
		#	voidHit=tck.hitCount[0]
		#k="all"
		#if tck.search == 0:
		#	tck.search+=1e-3
		#tn=float(hit)/tck.search
		#an=float(ack.hit[clickCount.SumMark])/ack.search
		#trate=float(tck.search-voidHit)/tck.search
		#arate=float(ack.search-ack.hitCount[0])/ack.search
		
		ack=tck
		anum=clickCount.dictSum(ack.albumHit)
		hnum=clickCount.dictSum(ack.hit)
		tnum=clickCount.dictSum(ack.topicHit)
		prate=[]
		for i in range(10):
			if i in ack.posHit:
				prate.append(float(ack.posHit[i])/ack.posShow[i])
			else:
				prate.append(0)
		ashow=clickCount.dictSum(ack.albumShow)
		arate=0
		if ashow > 0:
			arate=float(anum)/ashow
		tshow=clickCount.dictSum(ack.topicShow)
		trate=0
		if tshow > 0:
			trate=float(tnum)/tshow
		if not pn:
			pn=True
			print "日期\t点击数\t专辑点击数\t专辑菜谱点击比\t话题点击数\t话题菜谱点击比\t菜谱首位点击率\t菜谱第3位点击率\t菜谱第4位点击率\t菜谱第10位点击率\t专辑点击率\t话题点击率"
		print "%s\t%d\t%d\t%d\t%.3f\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f"%(file[-10:],ack.search,hnum,anum,float(anum)/(hnum+1e-12),tnum,float(tnum)/(hnum+1e-12),prate[0],prate[2],prate[3],prate[9],arate,trate)
		#print "%s\t%f\t%f"%(file[-10:],float(tck.search)/ack.search,trate/arate)
		#print "%s\t%d\t%d\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f"%(file[-10:],ack.search,ack.hit[clickCount.SumMark],tck.search,hit,(tck.search-voidHit),float(hit)/tck.search,trate,arate,tn/an,trate/arate)
	
def printClick(ack):
	anum=clickCount.dictSum(ack.albumHit)
	hnum=clickCount.dictSum(ack.hit)
	tnum=clickCount.dictSum(ack.topicHit)
	ashow=clickCount.dictSum(ack.albumShow)
	tshow=clickCount.dictSum(ack.topicShow)
	hasHit=ack.hasHit
	vnum=0
	if 0 in ack.hitCount:
		vnum=ack.hitCount[0]
	recipeHasHit=ack.search-vnum
	print "%d\t%d\t%.3f\t%d\t%.3f\t%d\t%.3f\t%d\t%.3f\t%d\t%.3f\t%d\t%.3f\t%d\t%.3f"%(ack.search,hasHit,hasHit/(ack.search+1e-12),hnum,hnum/(ack.search+1e-12),recipeHasHit,recipeHasHit/(ack.search+1e-12),anum,anum/(ashow+1e-12),ashow,ashow/(ack.search+1e-12),tnum,tnum/(tshow+1e-12),tshow,tshow/(ack.search+1e-12))
	if hasattr(ack.ms,"t"):
		print "topic\t%d\t%d\t%.3f"%(ack.ms.t,ack.ms.tb,float(ack.ms.tb)/(ack.ms.t+1e-12))

def countNew():
	ts={}
	#for line in open("/home/zhangzhonghui/data/backup.1203/tags.txt"):
	#	ts[line.strip()]=1
	#	ts["v45_"+line.strip()]=1
	ts={}
	ts["ck45_##total##"]=1
	ts["ck44_##total##"]=1
	ack=clickCount.Click()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		k=cols[0]
		if k.startswith(searchKeyword45.CardFix):
			continue
		if k not in ts:
			continue
		(k,click)=clickCount.readClick(cols)
		print k,
		printClick(click)
		ack.merge(click)
	printClick(ack)

if __name__ == "__main__":
	#count()
	countNew()

