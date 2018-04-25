#encoding=utf-8

import sys

sys.path.append("./")

qualitySmooth=200.0
logSmooth=2.0 #避免取对数后的结果为0
stepSmooth=10.0
photoRate=8.0
commentRate=5.0
likeRate=2.0

import math

def timeDiv(nowTime,dataTime):
	diff=(nowTime-dataTime)/(1000*60*60*24.0);
	return 1.0/(math.pow(diff, 0.5)+3.0);

def score(rate,cc,lc,vc,pc,step):
	lc*=likeRate
	cc*=commentRate
	pc*=photoRate
	likec=cc+lc+pc+logSmooth
	allc=likec+vc+qualitySmooth
	try:
		q=math.log(likec)/math.log(allc)
	except:
		sys.stderr.write("likec:%f,allc:%f"%(likec,allc))
		q=0.1
	q*=rate
	q*=math.log(step+stepSmooth)
	#print "rate=",rate,"cc",cc,"like",lc,"view",vc,"photo",pc,"step",step,q
	return q

import time

def quality():
	#db=DB()
	#cursor=db.execute("select recipeid,Title,Rate,CommentCount,LikeCount,ViewCount,Step,PhotoCount,CreateTime,Status from Recipe;")
	nowTime=time.time()
	#for r in cursor.fetchall():
	for line in sys.stdin:
		r=line.strip().split("\t")
		if len(r) < 10:
			continue
		status=r[9]
		if status != "0":
			continue
		title=r[1]
		if title== None or len(title) == 0:
			continue
		rate=r[2]
		if rate == None or len(rate) == 0 or rate == "NULL" or float(rate) <= 0:
			continue
		q=score(float(rate),float(r[3]),float(r[4]),float(r[5]),float(r[7]),float(r[6]))
		dataTime=time.mktime(time.strptime(str(r[8]),"%Y-%m-%d %H:%M:%S"))	
		q*=timeDiv(nowTime,dataTime)
		vc=int(r[4])+int(r[5])
		print r[0]+"\t"+title+"\t"+str(q)+"\t"+str(int(dataTime))+"\t"+str(int(float(rate)))+"\t"+str(vc)

def testTime():
	nowTime=time.time()
	tstr="2009-05-30 17:51:01"
	dataTime=time.mktime(time.strptime(tstr,"%Y-%m-%d %H:%M:%S"))
	print dataTime
	print timeDiv(nowTime,dataTime)

if __name__=="__main__":
	#testTime()
	quality()


