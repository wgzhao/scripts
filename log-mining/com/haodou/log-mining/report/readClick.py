#encoding=utf-8

import sys
sys.path.append("../log")
import clickCount
import cardClick
import searchKeyword45

sys.path.append("../util")
import DictUtil
import os

def readWeitHitRate(file):
	f=open(file)
	whr={}
	for line in f:
		p=line.find("\t")
		k=line[0:p]
		if k.startswith(searchKeyword45.CardFix):
			continue
		cols=line.strip().split("\t")
		try:
			(k,click)=clickCount.readClick(cols)
		except:
			sys.stderr.write(line)
			continue
		whr[k]=click.weitHitRate()	
	f.close()
	return whr

def readSomeWordMultiFile(dir,words):
	files=os.listdir(dir)
	files=sorted(files)
	cks={}
	for file in files:
		for line in open(dir+"/"+file):
			p=line.find("\t")
			k=line[0:p]
			if k not in words:
				continue
			cols=line.strip().split("\t")
			try:
				(k,click)=clickCount.readClick(cols)
				print line.strip()
				if k not in cks:
					cks[k]=click
				else:
					cks[k].merge(click)
			except:
				sys.stderr.write(line)
				continue
	for k in cks:
		print k+"\t"+str(cks[k])

def testReadWeitHitRate():
	whr=readWeitHitRate("/home/zhangzhonghui/data/backup.1203/searchKeyword45.2014-12-04")
	print "size:",len(whr)
	num=0
	for k in whr:
		if num > 10:
			continue
		num+=1
		print k,whr[k]
	
def testReadSomeWordMultiFile():
	dir="/home/zhangzhonghui/data/backup.1208"
	words=["v45_早餐","v45_快手菜"]
	words=["ck45_##total##","ck44_##total##"]
	readSomeWordMultiFile(dir,words)

if __name__=="__main__":
	#testReadWeitHitRate()
	testReadSomeWordMultiFile()



