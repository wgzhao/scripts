#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../util")

import columnUtil

sys.path.append("../")
import column

def t1():
	for line in sys.stdin:
		p0=line.find("<body>")
		p1=line.find("}",p0)
		print columnUtil.escapeUtf8(line[p0:p1])

#hdfs dfs -text /backup/CDA39907/001/$day/* | python tt.py t2
def t2():
	us={}
	nus={}
	ms={}
	nms={}
	for line in open("/home/zhangzhonghui/data/In.txt"):
		cols=line.strip().split()
		u=cols[2]
		us[u]={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if not column.valid(cols):
			continue
		uuid=column.uuid(cols)
		method=cols[column.METHOD_CID]
		if uuid not in us:
			nus[uuid]=1
			if method not in nms:
				nms[method]=1
			else:
				nms[method]+=1
			continue
		if method not in ms:
			ms[method]=1
		else:
			ms[method]+=1
	for m in ms:
		nn=0
		if m in nms:
			nn=nms[m]
		if ms[m] > 100:
			print m,ms[m],nn,float(ms[m])*len(nus)/float(nn+1e-12)/float(len(us))
	print "notIn user:",len(us)
	print "other user:",len(nus)

def diff():
	cs={}
	for line in sys.stdin:
		cols=line.strip().split()
		type=cols[0]
		channel=cols[1]
		uuid=cols[2]
		if type not in cs:
			cs[type]={}
		if channel not in cs[type]:
			cs[type][channel]={}
		if uuid not in cs[type][channel]:
			cs[type][channel][uuid]=1
		else:
			cs[type][channel][uuid]+=1
	for type in cs:
		for channel in cs[type]:
			sum=len(cs[type][channel])
			repeat=0
			for uuid in cs[type][channel]:
				if cs[type][channel][uuid] > 1:
					repeat+=1
			print "%s\t%s\t%d\t%d\t%.4f"%(type,channel,sum,repeat,float(repeat)/sum)
#
#f1中的用户，如果出现在f2中，则去掉
def reduceUser(f1,f2):
	u1={}
	for line in open(f1):
		u=line.strip().split()[0]
		u1[u]=1
	for line in open(f2):
		u=line.strip().split()[0]
		if u in u1:
			del u1[u]
	for u in u1:
		print u

import urllib
sys.path.append("../util/")
import columnUtil
def unquote():
	for line in sys.stdin:
		print urllib.unquote(line)

def merge():
	us={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if u not in us:
			print u
			us[u]=1

def splitFile():
	N=5000
	ns=[]
	k=0
	for line in sys.stdin:
		ns.append(line.strip())
		if len(ns) >= N:
			wf=open("uid."+str(k)+".txt","w")
			wf.write(",".join(ns)+"\n")
			wf.close()
			k+=1
			ns=[]
	if len(ns) > 0:
		wf=open("uid."+str(k)+".txt","w")
		wf.write(",".join(ns)+"\n")
		wf.close()

if __name__=="__main__":
	#t1()
	#t2()
	#diff()
	#reduceUser(sys.argv[1],sys.argv[2])
	#unquote()
	#merge()
	splitFile()

