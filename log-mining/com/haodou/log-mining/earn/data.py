#encoding=utf-8

import sys

sys.path.append("./")
from conf import *
sys.path.append("../util")
import Stack

class Data(object):
	def __init__(self):
		self.lineNum=0
		self.lastP=-1
		self.p=-1
		self.p1=-1  #下一阶段，用于计算滑差
		self.open=0
		self.high=0
		self.low=0
		self.close=0
		self.day=0
		self.time=0
		self.vol=0
		self.ask=0
		self.askVol=0
		self.bid=0
		self.bidVol=0
		self.money=0
		self.bank=0
		self.settle=False

staticData=Data()
lastStaticData=Data()

#凡是异常，均返回0
#看是否晚于日内某个时间点，如1500代表下午3点
def afterInDay(data,t):
	if data.time == 0:
		return 0
	if type(data.time) == str and  len(data.time) >= 4:
		hm="".join(data.time.split(":"))
		if int(hm[-4:]) > t:
			return 1
		else:
			return -1
	return 0

def valid(d,lastD,ExceptRate=2.0):
	if d.p <= 0:
		return False
	if lastD.p <= 0:
		lastD.p=d.p
	if d.p/lastD.p >= ExceptRate or lastD.p/d.p >= ExceptRate:
		return False
	return True

#
#20150302,09:14:00.200,  3630.000,     1667,  3629.000,       17,     0.000,        0,     0.000,        0,     0.000,        0,     0.000,        0,  3630.000,       59,     0.000,        0,     0.000,        0,     0.000,        0,     0.000,        0,     0.000,   1815363000.00,     165105.0
def readTick(line,d,lastD,conf):
	if d.lineNum == 0:
		d.lineNum+=1
		return lastD,None
	if d != None:
		lastD.p=d.p
		lastD.vol=d.vol
	try:
		cols=line.strip().split(",")
		d.day=cols[0].strip()
		d.time=cols[1].strip()
		d.p=float(cols[2].strip())
		d.vol=int(cols[3].strip())
		if len(cols) >=16:
			d.bid=float(cols[4].strip())
			d.bidVol=int(cols[5].strip())
			d.ask=float(cols[14].strip())
			d.askVol=int(cols[15].strip())
		d.money=float(cols[-2].strip())
		d.bank=float(cols[-1].strip())
		ret=d
		if not valid(d,lastD):
			ret=None
		d.lastP=lastD.p
		d.lineNum+=1
		return lastD,ret
	except Exception,e:
		sys.stderr.write(e.message+"\t"+line)
		return lastD,None

def readZJIF00(line,d,lastD,conf):
	if d.lineNum <=2:
		d.lineNum+=1
		return lastD,None
	if d != None:
		lastD.p=d.p
		lastD.vol=d.vol
	try:
		cols=line.strip().split()
		d.day=cols[0]
		d.time=cols[1]
		d.open=float(cols[2])
		d.high=float(cols[3])
		d.low=float(cols[4])
		d.close=float(cols[5])
		#if random.random() < 0.5:
		#	d.close+=1.0
		#else:
		#	d.close-=1.0
		d.vol=float(cols[6])
		d.lastP=lastD.p
		d.p=d.close
		return lastD,d
	except Exception,e:
		sys.stderr.write(e.message+"\t"+line)
		return lastD,None

def readZJ0616(line,d,lastD,conf):
	if d.lineNum <=2:
		d.lineNum+=1
		return lastD,None
	if d != None:
		lastD.p=d.p
		lastD.vol=d.vol
	try:
		cols=line.strip().split(",")
		d.day=cols[0]
		d.time=cols[1]
		d.open=float(cols[2])
		d.high=float(cols[3])
		d.low=float(cols[4])
		d.close=float(cols[5])
		d.vol=float(cols[6])
		d.lastP=lastD.p
		d.p=d.open
		return lastD,d
	except Exception,e:
		sys.stderr.write(e.message+"\t"+line)
		return lastD,None

def readStock(line,d,lastD,conf):
	if d.lineNum == 0:
		d.lineNum+=1
		return lastD,None
	if d != None:
		lastD.p=d.p
	try:
		if line.find(",") > 0:
			cols=line.strip().split(",")
		else:
			cols=line.strip().split()
		if len(cols) < 3:
			cols=line.strip().split()
		if len(cols) == 1:
			d.p=float(cols[0].strip())
			d.lastP=lastD.p
		else:
			d.day=cols[0].strip()
			d.time=cols[1].strip()
			d.p=float(cols[2].strip())
			d.lastP=lastD.p
		d.lineNum+=1
		return lastD,d
	except Exception,e:
		#sys.stderr.write(e.message+"\t"+line)
		return lastD,None

def readV2(line,d,lastD,conf):
	cols=line.strip().split()
	d.lastP=float(cols[0])
	d.p=float(cols[1])
	return lastD,d

def readMulti(line,d,lastD,conf):
	cols=line.strip().split()
	d.time=cols[0]
	d.lastP=d.p
	d.p=float(cols[conf.V1DataColumn])
	if len(cols) >= 4:
		d.settle=True
	else:
		d.settle=False
	return lastD,d

def readArbitrage(line,d,lastD,conf):
	cols=line.strip().split()
	d.time=cols[0]
	d.lastP=d.p
	d.p=float(cols[2])-float(cols[1])
	if len(cols) >= 4:
		d.settle=True
	else:
		d.settle=False
	return lastD,d

def readStockDay(line,d,lastD,conf):
	cols=line.strip().split()
	day=cols[0]
	lastD.p=d.p
	d.p=float(cols[1])
	d.lastP=d.p
	if not valid(lastD,d):
		return lastD,None
	if day[0:7] > conf.EndTime or day[0:7] < conf.StartTime:
		#sys.stderr.write(line)
		return lastD,None
	d.day=day
	d.open=float(cols[1])
	d.high=float(cols[2])
	d.low=float(cols[3])
	d.close=float(cols[4])
	d.vol=float(cols[5])
	return lastD,d

def readDCE(line,d,lastD,conf):
	cols=line.strip().split()
	lastD.p=d.p
	lastD.time=d.time
	lastD.vol=d.vol
	try:
		d.time=int(cols[1])
		d.p=float(cols[2])
		d.vol=int(cols[3])
	except:
		return lastD,None
	d.lastP=lastD.p
	if not valid(lastD,d):
		return lastD,None
	return lastD,d

dataFunc={
	"stock":readStock,
	"tick":readTick,
	"V2":readV2,
	"multi":readMulti,
	"arbitrage":readArbitrage,
	"ZJIF00":readZJIF00,
	"zj0616":readZJ0616,
	"stockDay":readStockDay,
	"dce":readDCE,
}

def readLine(line,conf,d=staticData,lastD=lastStaticData):
	return dataFunc[conf.DataType](line,d,lastD,conf)
	
def test():
	for line in sys.stdin:
		lastD,d=readLine(line,TickConf())
		if d != None:
			print d.p,d.bid,d.bidVol,d.ask,d.askVol

def readMonths():
	dir="/home/zhangzhonghui/libsvm/data/data/data/"
	files=os.listdir(dir)
	files=sorted(files)
	lastP=0
	lastP1=0
	lastDs=[]
	append=0
	lastFile=""
	for file in files:
		if not file.startswith("if"):
			continue
		if file.startswith("ifs"):
			continue
		sys.stderr.write(file+"\n")
		lastI=0
		ds=[]
		for line in open(dir+file):
			cols=line.strip().split()
			if len(cols) < 3:
				sys.stderr.write(line)
				continue
			day=cols[0]
			time=int("".join(day.split("/"))[-6:])*10000+int("".join(cols[1].split(":")))
			p=cols[2]
			if len(lastDs) > lastI:
				(t1,p1)=lastDs[lastI]
				if  t1 < time:
					while t1 < time:
						if lastI < len(lastDs)-1:
							lastI+=1
							(t1,p1)=lastDs[lastI]
						else:
							print "%d\t%s\t%s\t%d"%(time,p1,lastP,append)
							break
				if time == t1:
					if len(lastDs) == lastI+1:
						print "%d\t%s\t%s\t%d"%(time,p1,p,append)
					else:
						print "%d\t%s\t%s"%(time,p1,p)
					lastI+=1
				else:
					#sys.stderr.write("time:%d file:%s < t1:%d lastFile:%s\n"%(time,file,t1,lastFile))
					#print "%d\t%s\t%s"%(time,lastP1,p)
					pass
				lastP1=p1
			else:
				ds.append((time,p))
			lastP=p
		if len(lastDs) > lastI:
			(t1,p1)=lastDs[len(lastDs)-1]
			print "%d\t%s\t%s"%(t1,p1,lastP)
			append=1-append
			sys.stderr.write("out settle:%d\tfile:%s!\n"%(t1,file))
		lastDs=[]
		for k in ds:
			lastDs.append(k)
		ds=[]
		lastFile=file
	n=0
	for t,p in ds:
		n+=1
		if n == len(ds):
			print "%d\t%s\t%s\t%d"%(t,p,p,append)
		else:
			print "%d\t%s\t%s"%(t,p,p)
					

def testReadLine():
	for line in sys.stdin:
		d1,d2=readLine(line,basicConf)
		if d2 != None:
			print d2.open,d2.vol
		else:
			print d2

def testStockDay():
	for line in sys.stdin:
		readLine(line,StockDayConf())

if __name__=="__main__":
	#test()
	#readMonths()
	#testReadLine()
	testStockDay()


