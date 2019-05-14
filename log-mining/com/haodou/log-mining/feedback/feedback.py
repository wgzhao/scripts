# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 16:13:43 2015

@author: Administrator
"""
import sys
sys.path.append("../util")
import DictUtil

FeedDir="../feedback/"

def readDict(file):
	list=[]
	for line in open(file):
		cols=line.strip().split()
		if len(cols) < 1:
			continue
		list.append(cols);
	return list
	
def readClassDict():
	classDict={}
	list=readDict(FeedDir+"feedback.class.txt")
	del list[0]
	for cols in list:
		p=cols[0]
		if p not in classDict:
			classDict[p]={}
		if len(cols) >= 2:
			classDict[p][cols[1]]=1
	return classDict
	
def readSynDict():
	tDict={}
	wDict={}
	list=readDict(FeedDir+"feedback.syn.txt")
	del list[0]
	for t,w in list:
		if t not in tDict:
			tDict[t]={}
		if w not in wDict:
			wDict[w]={}
		tDict[t][w]=1
		wDict[w][t]=1
	return tDict,wDict
	
def readTagDict(wDict):
	list=readDict(FeedDir+"feedback.tag.txt")
	ts=list[0]
	del list[0]
	for cols in list:
		for i in range(len(cols)):
			w=cols[i]
			if w not in wDict:
				wDict[w]={}
			wDict[w][ts[i]]=1
	return wDict

def addEDict(d,eDict):
	for w in d:
		eDict[w]=1
		for w1 in d[w]:
			eDict[w1]=1

def readPatternDict(eleDict):
	for line in open(FeedDir+"feedback.pattern.txt"):
		if line.find("：") >= 0:
			continue
		cols=line.strip().split()
		for c in cols:
			eleDict[c]=1

def readAllDict():
	eleDict={}	
	classDict=readClassDict()
	addEDict(classDict,eleDict)	
	tDict,wDict=readSynDict()
	readTagDict(wDict)
	addEDict(wDict,eleDict)	
	readPatternDict(eleDict)
	return classDict,tDict,wDict,eleDict	
	
def testReadAllDict():
	f_handler=open(FeedDir+'feedback.log', 'w')
	sys.stdout=f_handler	
	classDict,tDict,wDict,eleDict=readAllDict()
	print DictUtil.dictStr(tDict)
	print DictUtil.dictStr(wDict)
	print DictUtil.dictStr(classDict)
	print DictUtil.dictStr(eleDict)
	
if __name__=="__main__":
	testReadAllDict()

