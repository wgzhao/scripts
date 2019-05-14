#encoding=utf-8

import sys
import math
import heapq

def merge(d,d1):
	for w in d1:
		if w not in d:
			d[w]=d1[w]
		else:
			d[w]+=d1[w]
	return d

def merge2(d,d1):
	for w in d1:
		if w not in d:
			d[w]=d1[w]
		else:
			merge(d[w],d1[w])
	return d

def topn(d,n):
	if len(d) < n:
		return d
	sd=sorted(d.items(),key=lambda e:e[1],reverse=True)
	nn=0
	nd={}
	for w,v in sd:
		nd[w]=v
		nn+=1
		if nn >= n:
			break
	return nd

def topnItems(d,n):
	sd=heapq.nlargest(n,d.items(),key=lambda e:e[1])
	return sd

def dictStr(d):
	s="{"
	nn=0
	for w in d:
		nn+=1
		s+=str(w)+":"
		if type(d[w]) == dict:
			s+=dictStr(d[w])
		else:
			s+=str(d[w])
		if nn < len(d):
			s+=","
	s+="}"
	return s

def listStr(d):
	s="["
	nn=0
	for e in d:
		s+=str(e)
		nn+=1
		if nn < len(d):
			s+=","
	s+="]"
	return s

def addOne(d,w):
	if w not in d:
		d[w]=1
	else:
		d[w]+=1

def sum(d):
	s=0
	for w in d:
		s+=d[w]
	return s

def sum2(d):
	s=0
	for w in d:
		s+=d[w]*d[w]
	return s

def testTopn():
	d={2:1,45:2,13:2,16:5}
	d1={2:3,4:1}
	print topn(d,2)
	print merge(d,d1)

def inn(d1,d2):
	sum=0
	for t in d1:
		if t in d2:
			sum+=d1[t]*d2[t]
	return sum

def cosine(d1,d2):
	s1=sum2(d1)
	s2=sum2(d2)
	s=inn(d1,d2)
	return s/(math.pow(s1+1e-12,0.5)*math.pow(s2+1e-12,0.5))

def sortedDictStr(d):
	sd=sorted(d.items(),key=lambda e:e[1],reverse=True)
	return str(sd)

def statis(vs):
	n=len(vs)
	if n <= 0:
		return 0,0,0,0
	sum=0
	sum2=0
	s0=0
	for v in vs:
		sum+=v
		sum2+=v*v
		if v < 0:
			s0+=1
	avg=float(sum)/n
	std=0
	if n > 1:
		x2=(sum2-n*avg*avg)/(n-1)
		std=math.pow(x2,0.5)
	return sum,avg,std,s0

if __name__ == "__main__":
	testTopn()


