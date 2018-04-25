#encoding=utf-8

import sys
import os
import dictInfo

import readParaConf

def readArg():
	dimension=""
	dirname="./"
	fix="methodInfo"
	names={}
	for i in range(1,len(sys.argv),1):
		arg=sys.argv[i]
		p=arg.find(":")
		v=arg
		if p > 0:
			n=arg[0:p]
			v=arg[p+1:]
		else:
			n=i
		print n,v
		if n == "n" or n == 1:
			names=readParaConf.readParaSetting(v)
		elif n == "d" or n == 2:
			dimension=v
		elif n == "r" or n == 3:
			dirname=v
		elif n == "f" or n == 4:
			fix=v
	tmpFiles=os.listdir(dirname)
	files=[]
	for f in tmpFiles:
		if f.startswith(fix):
			files.append(dirname+"/"+f)
	for method in names:
		if dimension != "" and dimension not in names[method]:
			names[method][dimension]=dimension
		if len(names[method]) <= 1:
			names[method]["sum"]="sum"
	for m in names:
		print m,names[m]
	return (names,files)

import re
def match(word,p):
	pp=re.compile(p)
	#if word.startswith("recipe.getcollectrecomment_request---私人") and p.startswith("recipe.getcollectrecomment_request---私人"):
	#	sys.stderr.write(word+"\t"+p+"\t"+str(pp.match(word))+"\n")
	if pp.match(word):
		return True
	return False

def readCount(files,names):
	ps={}
	for file in files:
		if True:
			ps[file]={}
			for line in open(file):
				cols=line.strip().split("\t")
				if len(cols) < 2:
					continue
				m=cols[0]
				has=False
				for name in names:
					nn=name
					if name.endswith("(tot)") or name.endswith("(all)"):
						nn=name[0:-5]
					if match(m,nn):	
						has=True
				if not has:
					continue
				if eval(cols[1]) == None:
					#sys.stderr.write(file+"\t"+line)
					continue
				ps[file][m]=eval(cols[1])
				avgStr=ps[file][m]["avg"]
				if avgStr == "":
					avgStr="0"
					ps[file][m]["avg"]=float(avgStr)
	return ps

def merge(tjs):
	if len(tjs) == 1:
		tj=tjs[0]
		if tj == None:
			tj={}
			tj["sum"]=0
			tj["num"]=0
			tj["exceptRate"]=0
			tj["avg"]=0
		if "num" not in tj:
			if "userNum" in tj:
				tj["num"]=tj["userNum"]
		if "csum" not in tj:
			if "exceptRate" in tj:
				tj["csum"]=tj["sum"]*(1.0-tj["exceptRate"])
			else:
				tj["csum"]=tj["sum"]
		if "exceptRate" not in tj:
			tj["exceptRate"] = float(tj["sum"]-tj["csum"])/tj["sum"]
		return tj
	dtj={}
	dtj["sum"]=0
	dtj["num"]=0
	dtj["entropy"]=0
	csum=0
	tops={}
	for tj in tjs :
		if tj == None:
			continue
		if "entropy" in tj and tj["entropy"] > dtj["entropy"]:
			dtj["entropy"]=tj["entropy"]
		dtj["sum"]+=tj["sum"]
		if "csum" in tj:
			csum+=tj["csum"]
		elif "exceptRate" in tj:
			csum+=(tj["sum"]-tj["exceptRate"]*tj["sum"])
		if "num" in tj:
			dtj["num"]+=tj["num"]
		elif "useNum" in tj:
			dtj["num"]+=tj["userNum"]
		if "top10" in tj:
			for (w,c) in tj["top10"]:
				if w not in tops:
					tops[w]=c
				else:
					tops[w]+=c
			dtj["top10"]=dictInfo.topn(tops,10)
	if csum == 0:
		csum=dtj["sum"]
	dtj["csum"]=csum
	dtj["exceptRate"]=float(dtj["sum"]-csum)/(dtj["sum"]+1e-16)
	dtj["avg"]=dtj["sum"]/(dtj["num"]+1e-16)
	dtj["cavg"]=csum/(dtj["num"]+1e-16)
	return dtj

def getHead(names):
	s="日期"
	for method in names:
		for dimension in names[method]:
			if dimension == readParaConf.NameKey:
				continue
			s+="\t"+names[method][readParaConf.NameKey]+names[method][dimension]
	return s

def printNum(v):
	if type(v) == str:
		if len(v) == 0:
			return v
		v=eval(v)
	if type(v) == int:
		return "%d"%(v)
	if type(v) == float:
		return "%.4f"%(v)
	return str(v)

def getVs(ps,names):
	print getHead(names)
	lastsum={}
	for key in sorted(ps.keys()):
		ms=ps[key]
		pos=key.find("/")
		key=key[pos+1:]
		if len(ms) > 0:
			s=""
			sum={}
			sum["sum"]=0
			sum["num"]=0
			sum["csum"]=0
			mtjs={}
			for m in names:
				tjs=[]
				if m.endswith("(all)") or m.endswith("(tot)"):
					tm=m[0:-5]
					for k in ms:
						if match(k,tm):
							tjs.append(ms[k])
					mtjs[m]=merge(tjs)
					if m.endswith("(tot)"):
						sum["sum"]+=mtjs[m]["sum"]
						sum["num"]+=mtjs[m]["num"]
						sum["csum"]+=mtjs[m]["csum"]
				else:
					for k in ms:
						if k==m:
							mtjs[m]=merge([ms[k]])
							sum["sum"]+=mtjs[m]["sum"]
							sum["num"]+=mtjs[m]["num"]
							sum["csum"]+=mtjs[m]["csum"]
							break
			for m in names:
				for dimension in names[m]:
					if dimension == readParaConf.NameKey:
						continue
					v=""
					if m in mtjs and dimension in mtjs[m]:
						v=mtjs[m][dimension]
					s+="\t"+printNum(v)
			print key+s
			lastsum=sum

if __name__=="__main__":
	(names,files)=readArg()
	ps=readCount(files,names)
	getVs(ps,names)

