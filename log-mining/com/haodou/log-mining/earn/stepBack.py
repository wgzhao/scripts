#encoding=utf-8

from conf import *
import sys
import os
import loss
import wave
import data
sys.path.append("../util")
import Stack

class State(object):
	def reset(self):
		self.LastClose5=False
		self.IsBull=False

	def __init__(self):
		self.reset()
	
	def __str__(self):
		return "%r %r"%(self.LastClose5,self.IsBull)

	def read(self,str):
		cols=str.split()
		if len(cols) < 2:
			self.reset()
			return
		self.LastClose5=eval(cols[0])
		self.IsBull=eval(cols[1])

def back(f,conf,guohu=1.0):
	phase=wave.Phase([100,conf.LargeSize,conf.Size,conf.MidSize])
	li=0
	lsmall=0
	bi=-1
	bn=0
	bprice=0
	lastV=-1
	lastLow=-1
	smallQueue=Stack.Queue(3,init=0)
	smallV=0
	navg=0
	flow=10000.0
	state=State()
	lastClose5=False
	lastIsBull=False
	for line in f:
		li+=1
		lastD,nowD=data.readLine(line,conf)
		if nowD == None:
			if bn > 0:
				bi=-1
				bn=0
				lastV=-1
				lastLow=-1
			continue
		if nowD.open <= 0 or nowD.close <= 0:
			if bn > 0:
				bi=-1
				bn=0
				lastV=-1
				lastLow=-1
			continue
		v=nowD.close
		high=nowD.high
		if lastV < 0:
			lastV=nowD.open
		add=(v-lastV)/float(lastV)
		if add > 0.11 or add < -0.11:
			bn=0
			bi=-1
			lastV=-1
			continue
		rate=(v-nowD.open)/float(nowD.open)
		hrate=(high-nowD.open)/float(nowD.open)
		hadd=(high-lastV)/float(lastV)
		oadd=(nowD.open-lastV)/float(lastV)
		avg=phase.avg(conf.MidSize)
		mavg=phase.avg(conf.LargeSize)
		avg100=phase.avg(100)
		SA=0.07
		#B12
		if bn > 0:
			#B1,B2,B3,B4
			p=nowD.close
			#B11,B12,B13,B14,B15
			if nowD.open > (1.0+SA)*lastV:
				p=nowD.open
			#B5,B6,B7,B8,B9,B10,B11,B12,B13,B14,B15
			elif nowD.high > (1.0+SA)*lastV:
				p=(1.0+SA)*lastV
			e=bn*100*(p-0.0013*p-guohu*0.6/1000-bprice)
			bn=0
			print "%s\t%f\t%s"%(nowD.day,e/flow,str(state))
			#curve.addE(e)
			bi=-1
		#elif bi > 0:
		#	if nowD.open < 1.06*lastV:
		#		bprice=nowD.open*(1.0+0.0003)+guohu*0.6/1000
		#		bn=int(curve.now/(100*bprice))
		if high > avg:
			navg+=1
		else:
			navg=0
		#B1,2,3,4,5
		#if (hrate > 0.085 or (hadd > 0.085 and hrate > 0.05) )and lsmall >= 1 and nowD.low <= avg and nowD.close > avg:
		#B6   增长率:0.790062 最大下降:0.228  @683    增长下降比:3.4653  0.6760年
		#if (hrate > 0.07 or (hadd > 0.07 and hrate > 0.05) )and lsmall >= 1 and nowD.low <= avg and nowD.close > avg:
		#B7
		HR=0.08 #加oadd < (0.095-HR)防止买不进
		#if hrate > HR and oadd < (0.095-HR) and lsmall >= 1 and nowD.low <= avg and nowD.close > avg:
		#B8,B9,B10,B11,B12,B13,B14,B15
		#HR=0.04
		HA=-0.02
		#B8
		#if hrate > HR and oadd < (0.095-HR) and lsmall >= 1 and lastV <= avg*1.02 and nowD.high > (1+HA)*avg:
		#B9
		#if hrate > HR and oadd < (0.095-HR) and lsmall >= 1 and lastV <= avg*1.02 and nowD.high > (1+HA)*avg:
		state.reset()
		#B10,B11,B12,B13,B15
		if hrate > HR and oadd < (0.095-HR) and lsmall >= 1 and lastV <= avg*1.02:
		#B14
		#if hrate > HR and oadd < (0.095-HR) and lsmall >= 1:
			state.LastClose5=lastClose5
			state.IsBull=lastIsBull
			bi=li
			#B1,2,3,4,5,6
			p=nowD.close
			#B7.1
			#HA=0.0
			#if hadd > HA: #两个条件要同时满足
			#	if nowD.open*(1+HR) <= (1.0+HA)*lastV:
			#		p=(1.0+HA)*lastV
			#	else:
					#sys.stderr.write("high open%.3f!\n"%(nowD.open))
			#B7,B10,B11,B12,B13,B14,B15,B16
			p=nowD.open*(1+HR)
			#B8,B9
			#if p < (1+HA)*avg:
			#	p=(1+HA)*avg
			bprice=p*(1.0+0.0003)+guohu*0.6/1000
			bn=int(flow/(100*bprice))
			#if add < 0.099 or rate > 0.5:
			#	bprice=nowD.close*(1.0+0.0003)+guohu*0.6/1000
			#	bn=int(curve.now/(100*bprice))
		phase.simpleAdd(nowD.close)
		smallV-=smallQueue.pop()
		#B12,B13,B14,B15
		lastClose5=False
		lastIsBull=False
		if mavg > avg100:
			lastIsBull=True
		#B1  0.48   0.25	
		#B1#if add < 0.04 and rate < 0.04 and rate > -0.06 and nowD.low <= avg and nowD.high >= avg:
		#B2#	增长率:0.55 最大下降:0.217   mrate:0.6321
		#if add < 0.04 and rate < 0.04	and nowD.low <= avg and nowD.high >= avg:
		#B3#	0.53 最大下降:0.157   0.6680年	mrate:0.6491
		#B4,B5,B6,B7,B8,B9,B10,B11,B12,B13,B14
		if add < 0.04 and rate < 0.02 and nowD.low <= avg*1.02 and nowD.high >= avg*0.98:
		#B15
		#if add < 0.04 and rate < 0.02 and nowD.low <= avg*1.03 and nowD.high >= avg*0.97:
			#B12,14
			#if nowD.open >= avg*0.98 and nowD.close <= avg*1.02 and nowD.low<= avg and nowD.high >= avg:
			#B13,B15
			if nowD.low<= avg and nowD.high >= avg:
				lastClose5=True
			lsmall+=1
			smallV+=1
			smallQueue.add(1)
		else:
			lsmall=0
			smallQueue.add(0)
		lastV=v


def testBack():
	import random
	dir="./stock_week"
	dir="./stock_day"
	fi=0
	conf=StockDayConf()
	for file in os.listdir(dir):
		#print file
		if random.random() < 0.0:
			continue
		guohu=1.0
		if file.startswith("sz"):
			guohu=0.0
		back(open(dir+"/"+file),conf,guohu)
		#print curve.time,curve.now
		fi+=1
	#sys.stderr.write("%s\n"%(curve.confStr(conf)))

if __name__=="__main__":
	testBack()

