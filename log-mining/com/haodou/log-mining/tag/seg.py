#encoding=utf-8

import sys

sys.path.append("../util")
from utf8 import *

import re

EngWord=re.compile(u'[0-9a-zA-Z_.%@#\\-]+')

class MaxSeg(object):	
	def __init__(self,maxSegLen=5):
		ds={}
		for line in uopen("../log/segDict.txt"):
			cols=line.strip().split("\t")
			ds[cols[0]]=int(cols[1])
		for line in uopen("../tag/code0327/mergeCateName.txt"):
			cols=line.strip().split("\t")
			w=cols[0]
			if w not in ds:
				ds[w]=1
		self.segDict=ds
		self.maxSegLen=maxSegLen

	seg=None
	@classmethod
	def getSeg(clazz):
		if clazz.seg == None:
			clazz.seg=MaxSeg()
		return clazz.seg
		
	@classmethod
	def getDict(clazz):
		if clazz.seg == None:
			clazz.seg=MaxSeg()
		return clazz.seg.segDict
	
	@classmethod
	def addW(clazz,w):
		d=clazz.getDict()
		if type(w) == str:
			w=un(w)
		if w not in d:
			d[w]=1

	def maxSeg(self,line):
		if type(line) == str:
			line=un(line)
		ws=[]
		i=0
		while i < len(line):
			s=i+self.maxSegLen
			if s > len(line):
				s=len(line)
			has=False
			for j in range(s,i,-1):
				w=line[i:j]
				if w in self.segDict:
					has=True
					break
			if has:
				word=line[i:j]			
				i=j
			else:
				word=line[i:i+1]
				i+=1
			if len(ws) > 0 and EngWord.match(ws[-1]) and EngWord.match(word):
				ws[-1]+=word
			else:
				ws.append(word)
		return ws

if __name__=="__main__":
	ws=MaxSeg.getSeg().maxSeg("牛肉切丝12-aaz_45.u")
	for w in ws:
		print en(w)
	
