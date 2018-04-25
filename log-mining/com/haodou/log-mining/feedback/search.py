# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 08:59:07 2015

@author: Administrator
"""

import sys
sys.path.append("../")
import column
sys.path.append("../tag")
import seg
sys.path.append("../util")
import utf8
import DictUtil
import math
import feedback
import DBUtil2
import MySQLdb

FeedDir="../feedback/"
ColsNum=8

class Feed(object):
	def __init__(self,uuid,createTime,content,channel,version):
		self.uuid=uuid
		self.createTime=createTime
		self.content=content
		self.channel=channel
		self.version=version
		self.tags={}
	
	def addTag(self,tag):
		self.tags[tag]=1

	def tagStr(self):
		s="["
		n=0
		for t in self.tags:
			if n > 0:s+=","
			n+=1	
			s+=t
		s+="]"
		return s

	def __str__(self):
		return "%s\t%s\t%s\t%s\t%s\n标签:%s"%(self.content,self.uuid,self.createTime,self.channel,self.version,self.tagStr())

#Id,UUID,AppId,Status,Content,PhoneInfo,DelStatus,CreateTime
def parseFeedLine(line,repeats={}):
	cols=line.strip().split("\",\"")
	if len(cols) < ColsNum:
		return None
	uuid=cols[1]
	content=cols[4]
	channel=column.getValue(cols[5],"channel_type")[3:-1]
	if channel.startswith('"'):channel=channel[1:]
	createTime=cols[7][0:-1]
	version=column.getValue(cols[5],"soft_version")[3:-1]
	if version.startswith('"'): version=version[1:]
	feed=Feed(uuid,createTime,content,channel,version)
	if uuid+"_"+content in repeats:
		return None
	repeats[uuid+"_"+content]=1
	return feed

def readFeedback(lineOrFeeds,eleDict,begin="0000-00-00 00:00:00",end="9999-99-99 00:00:00"):
	signs={}
	for line in open(FeedDir+"sign.txt"):
		w=line.strip()
		if len(w) == 0:
			continue
		signs[utf8.un(line.strip())]=1
	signs[u" "]=1
	signs[u"\t"]=1
	n=0
	wordCount={}
	biCount={}
	for e in eleDict:
		seg.MaxSeg.addW(e)
	repeats={}
	for ele in lineOrFeeds:
		n+=1
		if n <=0:continue
		if type(ele) == Feed:
			feed=ele
		else:
			feed=parseFeedLine(ele,repeats)
		if feed == None or (feed.createTime < begin or feed.createTime > end):
			#if feed != None:print feed.createTime
			continue
		ws=seg.MaxSeg.getSeg().maxSeg(feed.content)
		dws={}
		lastW=""
		bn=0
		for w in ws:
			if w not in dws:
				DictUtil.addOne(wordCount,w)
				dws[w]=1
			if bn > 0 and w not in signs and lastW not in signs:
				if lastW+w not in dws:
					DictUtil.addOne(biCount,lastW+w)
					dws[lastW+w]=1
			bn+=1
			lastW=w
		if n < 10:print feed
		#else:break
	for bi in biCount:  #二元组合打折计数
		bc=biCount[bi]
		if bi not in wordCount:
			if bc > 3:wordCount[bi]=bc
		else:
			wordCount[bi]+=bc
	return wordCount
	
def selectW(wordCount,backDict,eleDict={}):
	allList=[]
	wordTfidf={}
	for w in wordCount:
		if wordCount[w] <= 2: #抑制低频
			tf=math.pow(wordCount[w],0.5)*0.5
		else:
			tf=math.pow(wordCount[w],0.7)
		idf=1.0/math.log(2.0)
		if w in backDict:
			idf=1.0/math.log(backDict[w]+1.0)
		if w in eleDict:
			idf=2.0
		wordTfidf[w]=tf*idf
	items=sorted(wordTfidf.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
	n=0
	stopwords={}
	for line in open(FeedDir+"stopword.txt"):
		stopwords[line.strip()]=1
	stopwords[" "]=1
	stopwords["\t"]=1
	#sf=open("stopword.txt","a")	
	print len(items),len(wordCount)
	for w,v in items:
		w8=utf8.ustr(w)
		if w8 in stopwords:
			continue
		n+=1
		allList.append((utf8.ustr(w),v,wordCount[w]))
	return allList
	
def phaseW(begin,end,MaxN=2000):
	classDict,tDict,wDict,eleDict=feedback.readAllDict()
	backDict=seg.MaxSeg.getDict()
	lastWc=readFeedback(open(FeedDir+"feedback.csv"),eleDict,"0",begin)
	allW=selectW(lastWc,backDict,eleDict)
	allDict={}
	for w,v,k in allW:
		allDict[w]=k
	wordCount=readFeedback(open(FeedDir+"feedback.csv"),eleDict,begin,end)
	phaseDict=selectW(wordCount,backDict,eleDict)
	n=len(phaseDict)/2
	if n > MaxN:
		n=MaxN
	newList=[]
	for i in range(len(phaseDict)):
		w,v,k=phaseDict[i]
		if i >= n and w not in eleDict:
			continue
		an=0
		if w in allDict:
			an=allDict[w]
		v1=v*1.0/math.log(an+2.0)
		newList.append((w,v1,k,v,an))
		if i < 30:
			print w,v,v1,k,an
	return newList
	#return sorted(newList, lambda x, y: cmp(x[1], y[1]), reverse=True)


def searchW(word,begin="0000-00-00 00:00:00",end="9999-99-99 00:00:00"):
	ws=word.split()
	feeds=[]
	if len(word) == 0:
		return lines
	repeats={}
	n=0
	for line in open(FeedDir+"feedback.csv"):
		n+=1
		if n <=0:continue
		feed=parseFeedLine(line,repeats)
		if feed == None  or (feed.createTime < begin or feed.createTime > end):
			continue
		has=True
		for w in ws:
			if feed.content.find(w) < 0:
				has=False
		if has:
			feeds.append(feed)
	rfeeds=[]
	for i in range(len(feeds)):
		rfeeds.append(feeds[len(feeds)-i-1])
	eleDict={}
	feedback.readPatternDict(eleDict)
        backDict=seg.MaxSeg.getDict()
	SubN=5
	wordCount=readFeedback(rfeeds,eleDict)
	phaseDict=selectW(wordCount,backDict,eleDict)
	n=0
	subs=[]
	for w,v,k in phaseDict:
		if word.find(w) >= 0:
			continue
		n+=1
		sn=0
		if n > SubN and w not in eleDict and k < 1+n/2:
			continue
		for fi in range(len(rfeeds)):
			feed=rfeeds[fi]
			if feed.content.find(w) >= 0:
				sn+=1
				feed.addTag(w)
		if sn > (len(rfeeds)/SubN) or sn >=1+n/2:
			 subs.append((w,sn))	
	return (rfeeds,subs)
	
if __name__=="__main__":
	#f_handler=open(FeedDir+'feedback.log', 'w')
	#sys.stdout=f_handler
	phaseW("2015-01-01 00:00:00","2015-12-31 00:00:00")
	feeds,subs=searchW("下载")
	for fi in range(len(feeds)):
		feed=feeds[fi]
		if fi > 10:break
		print feed
	print "#all",len(feeds)
	for w,sn in subs:
		print w,sn
	#dbTest()

