#encoding=utf-8

import loss
import sys
from conf import *
sys.path.append("../util")
import DictUtil
import random
import stepBack

TrendFac=2.5
Close5Fac=1.0

def monthDiv(firstMonth,lastMonth):
	return (lastMonth/100-firstMonth/100)*12+(lastMonth%100-firstMonth%100+1)

def dayDiv(firstMonth,lastMonth):
	return (lastMonth/100-firstMonth/100)*250+(lastMonth%100-firstMonth%100+1)*21

def output(lastDay,es,curve,conf):
	BankRate=0.8
	SumE=0
	buy=0
	#min=150
	#max=250
	K=30
	#Base=(max+min)*(max-min)/(2.0*K)
	Base=K/400.0
	n=0
	sum=0
	last=0
	random.shuffle(es)
	for e,fac in es:
		n+=fac
		#if n > min:
		#	sum+=n
		if n - last > Base:
			#print "sum",sum,"last",last,"SumE",SumE,"e",e,"buy",buy
			SumE+=1.0*e
			buy+=1.0
			last=n
		if buy >= K:
			break
	e=0
	if buy > 0:
		e=BankRate*SumE/float(buy)
	print "%s\t%.4f\t%.4f\t%d\t%.4f\t%d"%(lastDay,curve.now,e,len(es),SumE,buy)
	curve.addTime()
	if buy > 0:
		curve.addWin(e*curve.now,conf)
	curve.addE(e*curve.now)


def combine(f,conf,start=0,end=10000):
	lastDay=""
	curve=loss.Loss(1.0)
	firstMonth=-1
	lastMonth=0
	lastValue=1.0
	vs=[]
	yvs=[]
	lastYear=0
	lastYearValue=1.0
	es=[]
	state=stepBack.State()
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		day=cols[0]
		year=int(cols[0].split("-")[0])
		if year < start or year > end:
			continue
		month=int(cols[0].split("-")[1])
		month=year*100+month
		if firstMonth <= 0:
			firstMonth=month
		if lastMonth <= 0:
			lastMonth=month
			lastYear=year
		if lastMonth!=month:
			rate=curve.now/lastValue-1.0
			sys.stderr.write("%d-%d\t%.2f%%\n"%(lastMonth/100,lastMonth%100,rate*100))
			vs.append(rate)
			lastMonth=month
			lastValue=curve.now
		if lastYear != year:
			rate=curve.now/lastYearValue-1.0
			yvs.append((lastYear,rate))
			lastYear=year
			lastYearValue=curve.now
		if lastDay == "":
			lastDay=day
		if lastDay != day:
			output(lastDay,es,curve,conf)
			lastDay=day
			es=[]
		e=float(cols[1])
		state.read(cols[2])
		if state.IsBull:
			fac=TrendFac
		else:
			fac=1.0/TrendFac
		if state.LastClose5:
			fac*=Close5Fac
		else:
			fac/=Close5Fac
		es.append((e,fac))
	if lastDay != "":
		rate=curve.now/lastValue-1.0
		sys.stderr.write("%d-%d\t%.2f%%\n"%(lastMonth/100,lastMonth%100,rate*100))
		vs.append(rate)
		rate=curve.now/lastYearValue-1.0
		yvs.append((lastYear,rate))
		output(lastDay,es,curve,conf)
		es=[]
	sys.stderr.write("firstMonth:%d\tlastMonth:%d\n"%(firstMonth,lastMonth))
	curve.setTime(dayDiv(firstMonth,lastMonth))
	sys.stderr.write(curve.confStr(conf)+"\n")
	(sum,avg,std,s0)=DictUtil.statis(vs)
	mrate=math.pow(curve.now,1.0/monthDiv(firstMonth,lastMonth))-1.0
	sys.stderr.write("月均统计	sum:%.4f	loss:%d,%.2f	avg:%.2f%%,%.2f%%	std:%.4f	avg/std:%.4f\n"%(sum,s0,float(s0)/len(vs),avg*100,mrate*100,std,mrate/std))
	rvs=[]
	for lastYear,rate in yvs:
		rvs.append(rate)
		sys.stderr.write("%d\t%.2f%%\n"%(lastYear,rate*100))
	(sum,avg,std,s0)=DictUtil.statis(rvs)
	yrate=curve.rate
	sys.stderr.write("年均统计  sum:%.4f    loss:%d,%.2f	avg:%.2f%%,%.2f%%    std:%.4f    avg/std:%.4f\n"%(sum,s0,float(s0)/len(vs),avg*100,yrate*100,std,yrate/std))

def testCombine():
	combine(sys.stdin,StockDayConf())

if __name__=="__main__":
	start=0
	end=10000
	if len(sys.argv) >= 2:
		start=int(sys.argv[1])
	if len(sys.argv) >= 3:
		end=int(sys.argv[2])
	combine(sys.stdin,StockDayConf(),start=start,end=end)

