#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../util/")
sys.path.append("util/")

import os
import re
import TimeUtil
import time
from datetime import datetime,date,timedelta


def dayFormat(s):
	p=re.compile("20[0-9]{2}\\-[0-1][0-9]\\-[0-3][0-9]")
	if p.match(s):
		return True
	return False

def getFiles(hdfsDir,tmpFile="tmp.hdfsFile.getFiles"):
	os.system("hdfs dfs -ls "+hdfsDir+" >"+tmpFile+";")
	flist=[]
	for line in open(tmpFile):
		ss=line.strip().split()
		if len(ss) < 8:
			continue
		flist.append(ss[7])
	return flist

def getDays(hdfsDir):
	flist=getFiles(hdfsDir)
	days=[]
	for f in flist:
		p=f.rfind("/")
		d=f[p+1:]
		if dayFormat(d):
			days.append(d)
	return sorted(days)

def getLastDay(hdfsDir,N=7):
	sortedDays=getDays(hdfsDir)
	if len(sortedDays) > 0:
		lastDay=TimeUtil.addDay(sortedDays[-1],1)
		return lastDay
	else:
		return datetime.strftime(date.today()-timedelta(days=N),"%Y-%m-%d")

def delDays(hdfsDir,N=3):
	sortedDays=getDays(hdfsDir)
	s=len(sortedDays)-N
	if s>0:
		for i in range(0,s,1):
			os.system("hdfs dfs -rm -r "+hdfsDir+"/"+sortedDays[i]+";")

def getInputDaysStr(sortedDays,nowDay,baseDir,N=7):
	baseFiles=getFiles(baseDir)
	baseFileDict={}
	for f in baseFiles:
		baseFileDict[f]=1
	if len(sortedDays) > 0:
		lastDay=TimeUtil.addDay(sortedDays[-1],1)
		if not baseDir.endswith("/"):
			baseDir=baseDir+"/"
		inputDir=""
		while lastDay <= nowDay:
			if len(inputDir) > 0:
				inputDir+=","
			newDir=baseDir+lastDay
			if newDir not in baseFileDict:
				sys.stderr.write(newDir+"is not exists!\n")
				continue
			inputDir+=newDir
			lastDay=TimeUtil.addDay(lastDay,1)
	else:
		inputDir=""
		for i in range(N):
			if len(inputDir) > 0:
				inputDir+=","
			newDir=baseDir+datetime.strftime(date.today()-timedelta(days=i+1),"%Y-%m-%d")
			if newDir not in baseFileDict:
				sys.stderr.write(newDir+"is not exists!\n")
				continue
			inputDir+=newDir
	return inputDir

def getInputStr(hdfsDir,inputBaseDir="/backup/CDA39907/001/",N=7):
	sortedDays=getDays(hdfsDir)
	return getInputDaysStr(sortedDays,TimeUtil.getYesterdayStr(),inputBaseDir,N)

def getInputStrPlus(hdfsDir,inputBaseDir="/backup/CDA39907/001/",N=7):
	sortedDays=getDays(hdfsDir)
	nowPath=getInputDaysStr(sortedDays,TimeUtil.getYesterdayStr(),inputBaseDir,N)	
	if len(sortedDays) > 0:
		return hdfsDir+"/"+sortedDays[-1]+","+nowPath
	else:
		return nowPath

def test():
	hdfsDir="/user/zhangzhonghui/"
	sortedDays=getDays(hdfsDir)
	print sortedDays
	delDays(hdfsDir)
	#print getInputDays(sortedDays,TimeUtil.getTodayStr(),"/user/zhangzhonghui/logcount/tmp")
	print getInputStr(hdfsDir)
	print getLastDay("/user/zhangzhonghui/userRecom/",N=7)

if __name__=="__main__":
	test()
	

