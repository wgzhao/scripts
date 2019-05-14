import sys
import os

sys.path.append("../log")
import clickCount
import searchKeyword45
sys.path.append("../util")
import DictUtil

def count():
	ts={}
	tagFile="/home/zhangzhonghui/data/backup.1203/tagQuery.all.txt"
	#tagFile="/home/zhangzhonghui/data/backup.1202/tag.txt"
	for line in open(tagFile):
		ts[line.strip()]=1
		ts["v45_"+line.strip()]=1
	dir=sys.argv[1]
	files=os.listdir(dir)
	files=sorted(files)
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
			if k.endswith("##total##"):
				(k,click)=clickCount.readClick(cols)
				ack.merge(click)
				continue
			if k not in ts:
				continue
			try:
				(k,click)=clickCount.readClick(cols)
			except:
				sys.stderr.write(line)
				continue
			tck.merge(click)
			voidHit=0
			if 0 in click.hitCount:
				voidHit=click.hitCount[0]
			hit=DictUtil.sum(click.hit)
			if clickCount.SumMark in click.hit:
				hit=click.hit[clickCount.SumMark]
			if click.search==0:
				click.search+=1e-12
			#print k,click.search
			#print "%s\t%s\t%d\t%d\t%d\t%f\t%f"%(file[-10:],k,click.search,hit,(click.search-voidHit),float(hit)/click.search,float(click.search-voidHit)/click.search)
			#print click
		hit=clickCount.dictSum(tck.hit)
		voidHit=0
		if 0 in tck.hitCount:
			voidHit=tck.hitCount[0]
		k="all"
		if tck.search == 0:
			tck.search+=1e-3
		tn=float(hit)/tck.search
		an=float(ack.hit[clickCount.SumMark])/ack.search
		trate=float(tck.search-voidHit)/tck.search
		arate=float(ack.search-ack.hitCount[0])/ack.search
		
		#print "%s\t%f\t%f"%(file[-10:],float(tck.search)/ack.search,trate/arate)
		print "%s\t%d\t%d\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f"%(file[-10:],ack.search,ack.hit[clickCount.SumMark],tck.search,hit,(tck.search-voidHit),float(hit)/tck.search,trate,arate,tn/an,trate/arate)

def countNew():
	ts={}
	tagFile="/home/zhangzhonghui/data/backup.1203/tagQuery.all.txt"
	for line in open(tagFile):
		ts["v45_"+line.strip()]=1
		#ts[line.strip()]=1
	tck=clickCount.Click()
	ack=clickCount.Click()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		k=cols[0]
		if k.startswith(searchKeyword45.CardFix):
			continue
		if k.endswith("ck45_##total##"):
			(k,click)=clickCount.readClick(cols)
			ack.merge(click)
			continue
		if k not in ts:
			continue
		(k,click)=clickCount.readClick(cols)
		tck.merge(click)
	
	hit=clickCount.dictSum(tck.hit)
	voidHit=0
	if 0 in tck.hitCount:
		voidHit=tck.hitCount[0]
	tn=float(hit)/tck.search
	an=float(ack.hit[clickCount.SumMark])/ack.search
	trate=float(tck.search-voidHit)/tck.search
	arate=float(ack.search-ack.hitCount[0])/ack.search
	print "%d\t%d\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f"%(ack.search,ack.hit[clickCount.SumMark],tck.search,hit,(tck.search-voidHit),float(hit)/tck.search,trate,arate,tn/an,trate/arate)

if __name__ == "__main__":
	#count()
	countNew()


