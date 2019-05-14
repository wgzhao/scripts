import sys
import math

sys.path.append("./")

import ccinfo

def entropy(cc):
	if len(cc) == 0:
		return 0
	sum=0.0
	for t in cc:
		if cc[t] <= 0:
			continue
		sum+=cc[t]
	ent=0
	for t in cc:
		if cc[t] <= 0:
			continue
		ent-=math.log(cc[t]/sum)*cc[t]/sum
	return ent

def topn(d,n,cc=None):
	if cc == None:
		cc=ccinfo.dict2cc(d)
	sortedcc=[(c,cc[c]) for c in sorted(cc.keys(),reverse=True)]
	k=0
	cut=0
	cutc=-1
	for c,nc in sortedcc:
		k+=nc
		if k >= n:
			cutc=c
			cut=k-n
			break
	top={}
	tmpCut=0
	for w in d:
		c=d[w]
		if c > cutc:
			top[w]=c
		elif c == cutc:
			tmpCut+=1
			if tmpCut > cut:
				top[w]=c
	return sorted(top.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)

def info(d):
	cc=ccinfo.dict2cc(d)
	info=ccinfo.info(cc)
	if info == None:
		return None
	info["top10"]=topn(d,10,cc)
	return info

def staticInfo(us):
	info={}
	if len(us) == 0:
		return info
	sum=0
	max=0
	min=1000000
	sum2=0
	csum2=0
	for u in us:
		sum+=us[u]
		sum2+=math.pow(us[u],2)
		if us[u] > max:
			max=us[u]
		if us[u] < min:
			min=us[u]
	info["min"]=min
	info["max"]=max
	avg=float(sum)/len(us)
	#strAvg="%.4f"%avg
	#while strAvg.endswith("0") or strAvg.endswith(".") :
	#	strAvg=strAvg[0:-1]
	#if strAvg == "":
	#	strAvg="0"
	info["avg"]=avg
	info["sum"]=sum
	sdev=math.pow(float(sum2)/len(us)-avg*avg,0.5)
	info["sum2"]=sum2
	info["sdev"]=sdev
	#1.96 is 95% confidence range
	if len(us) > 1:
		div=sdev/math.pow(len(us)-1,0.5)*1.96
		info["avgCeil"]=avg+div
		info["avgFloor"]=avg-div
	cmax=10*(int(avg)+1)
	if cmax < max:
		csum=0
		for u in us:
			if us[u] > cmax:
				csum+=cmax
				csum2+=math.pow(cmax,2)
			else:
				csum+=us[u]
				csum2+=math.pow(us[u],2)
		info["cmax"]=cmax
		info["csum"]=csum
		cavg=float(csum)/len(us)
		info["csum2"]=csum2
		info["csdev"]=math.pow(float(csum2)/len(us)-cavg*cavg,0.5)
	info["userNum"]=len(us)
	info["entropy"]=entropy(us)
	return info

def mergeInfo(f):
	minfo={}
	sum=0
	max=0
	min=1000000
	csum=0
	size=0
	sum2=0
	csum2=0
	for line in f:
		info=eval(line.strip())
		sum+=info["sum"]
		if info["max"] > max:
			max=info["max"]
		if info["min"] < min:
			min=info["min"]
		size+=info["userNum"]
		if "csum" in info:
			csum+=info["csum"]
		else:
			csum+=info["sum"]
		if "sum2" in info:
			sum2+=info["sum2"]
			if "csum2" in info:
				csum2+=info["csum2"]
			else:
				csum2+=info["sum2"]
	if sum <= 0:
		return {}
	minfo["sum"]=sum
	minfo["max"]=max
	minfo["min"]=min
	minfo["avg"]=float(sum)/(size)
	avg=minfo["avg"]
	minfo["userNum"]=size
	minfo["csum"]=csum
	minfo["cavg"]=float(csum)/(size)
	cavg=minfo["cavg"]
	if "sum2" in info:
		minfo["sum2"]=sum2
		minfo["csum2"]=csum2
		minfo["sdev"]=math.pow(float(sum2)/(size)-avg*avg,0.5)
		minfo["csdev"]=math.pow(float(csum2)/(size)-cavg*cavg,0.5)
	return minfo

if __name__=="__main__":
	if len(sys.argv) >= 2 and sys.argv[1] == "merge":
		minfo=mergeInfo(sys.stdin)
		if minfo != None:
			s=str(minfo["avg"])+"\t"+str(minfo["sum"])+"\t"+str(minfo["userNum"])+"\t"+str(minfo["max"])+"\t"+str(minfo["min"])+"\t"+str(minfo["csum"])+"\t"+str(minfo["cavg"])
			if "sum2" in minfo:
				s+="\t"+str(minfo["sum2"])+"\t"+str(minfo["csum2"])+"\t"+str(minfo["sdev"])+"\t"+str(minfo["csdev"])
			print s

