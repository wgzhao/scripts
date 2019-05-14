#encoding=utf-8

import sys

sys.path.append("./")
import posHit
import readTagWap
tagWaps=readTagWap.read()

def readRet(f,type):
	nn=0
	cs={}
	for line in f:
		if nn == 0: #忽略表头
			nn+=1
			continue
		cols=line.strip().split("\t")
		if len(cols) < 3:
			#sys.stderr.write(line)
			continue
		stype=cols[0]
		if stype.find(type) < 0:
			continue
		if len(stype) == 0:
			continue
		#print stype.strip()
		if stype not in cs:
			cs[stype]={}
		version=cols[1]
		cs[stype][version]=posHit.PosRet.read(cols[2:])
	return cs

def readTagWapRet(f):
	cs=readRet(f,"tag.wap")
	tagItems={}
	for tid in tagWaps:
		(name,items)=tagWaps[tid]
		tagItems[name]=items
	for stype in cs:
		s=stype.find("tag.wap_")+len("tag.wap_")
		e=stype.find(".",s)
		if e < 0:
			e=stype.find("_",s)
		tag=stype[s:e]
		if tag not in tagItems:
			#sys.stderr.write(stype+"\n")
			continue
		items=tagItems[tag]
		for version in cs[stype]:
			try:
				if int(version) < 480:
					continue
			except:
				#sys.stderr.write("version:"+version+"\n")
				continue
			if version != "480":
				continue
			ret=cs[stype][version]
			(hasHit,hit)=ret.hitNum()
			print "%s\t进入数:%d\t有点击数:%d\t有点击率：%.4f\t点击数：%d\t平均点击数:%.4f"%(tag,ret.num,hasHit,float(hasHit)/(ret.num+1e-32),hit,float(hit)/(ret.num+1e-32))
			for tab in items:
				for topic in items[tab]:
					for item in items[tab][topic]:
						if item in ret.sons:
							print "%s\t%s\t%s\t%s\t%d\t%d"%(tag,tab,topic,item,ret.sons[item][1],ret.sons[item][2])

def readT2(f,type):
	cs=readRet(f,type)
	tret=posHit.PosRet()
	for stype in cs:
		for version in cs[stype]:
			if version != "all_version":
				continue
			ret=cs[stype][version]
			ret=cs[stype][version]
			tret.merge(ret)
			'''
			(hasHit,hit)=ret.hitNum()
			print "%s\t进入数:%d\t有点击数:%d\t有点击率：%.4f\t点击数：%d\t平均点击数:%.4f"%(stype,ret.num,hasHit,float(hasHit)/(ret.num+1e-32),hit,float(hit)/(ret.num+1e-32))
			for son in ret.sons:
				print "%s\t%s\t%d\t%d"%(stype,son,ret.sons[son][1],ret.sons[son][2])
			'''
	(hasHit,hit)=tret.hitNum()
	print "进入数:%d\t有点击数:%d\t有点击率：%.4f\t点击数：%d\t平均点击数:%.4f"%(tret.num,hasHit,float(hasHit)/(tret.num+1e-32),hit,float(hit)/(tret.num+1e-32))

if __name__=="__main__":
	if sys.argv[1] == "wap":
		readTagWapRet(sys.stdin)
	else:
		readT2(sys.stdin,sys.argv[1])


