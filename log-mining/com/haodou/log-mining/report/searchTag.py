#encoding=utf-8

import sys
sys.path.append("../util")
import DBCateName
import DictUtil

def readLine(f):
	cates=DBCateName.readCateFile(open("/home/zhangzhonghui/log-mining/com/haodou/log-mining/util/cateidName.txt"))
	counts={}
	tc={}
	for line in f:
		if line.find("moreSearch") < 0:
			continue
		cols=line.strip().split("\t")
		if len(cols) < 11 or cols[-10] != "moreSearch":
			continue
		#print cols[-10],cols[-9],cols[-11]
		k=cols[0]
		if True:
		#if k != "ck45_##total##":
			ts=eval(cols[-3])
			if type(ts) == int:
				print line
				print cols[-4]
				print ts
			tshow=eval(cols[-2])
			rtn=int(cols[-1])
			if k not in counts:
				counts[k]=[0,0,{},{},0]
			counts[k][0]+=int(cols[-9])
			counts[k][4]+=rtn
			tns=counts[k][2]
			tnShow=counts[k][3]
			sum=0
			for t in ts:
				sum+=ts[t]
				tn=cates[int(t)]
				if tn not in tns:
					tns[tn]=0
				if tn not in tc:
					tc[tn]=[0,0]
				if not k.startswith("ck45"):
					tc[tn][0]+=ts[t]
				tns[tn]+=ts[t]
			showNum=0
			for t in tshow:
				if tshow[t] > showNum:
					showNum=tshow[t]
				tn=cates[int(t)]
				if tn not in tc:
					tc[tn]=[0,0]
				if not k.startswith("ck45"):
					tc[tn][1]+=int(cols[-9])
				if tn not in tnShow:
					tnShow[tn]=0
				tnShow[tn]+=tshow[t]
			counts[k][1]+=showNum
			#print "%s\t%s\t%s\t%s\t%.4f"%(cols[0],cols[32],cols[33],DictUtil.dictStr(tns),sum/(int(cols[32])+1e-32))

	for k in counts:
		(c,hc,ts,tshow,rtn)=counts[k]
		sum=0
		for tn in ts:
			sum+=ts[tn]
		ssum=0
		for tn in tshow:
			ssum+=tshow[tn]
		if ssum <= 0:
			continue
		print "%s\t%d\t%d\t%.4f\t%d\t%.4f\t%d\t%.4f\t%s\t%s"%(k,sum,c,sum/(c+1e-2),hc,sum/(hc+1e-2),rtn,rtn/(c+1e-12),DictUtil.dictStr(ts),DictUtil.dictStr(tshow))

	for t in tc:
		(v,s)=tc[t]
		print "%s\t%d\t%d\t%.4f"%(t,v,s,v/float(s+1e-2))

if __name__=="__main__":
	readLine(sys.stdin)

