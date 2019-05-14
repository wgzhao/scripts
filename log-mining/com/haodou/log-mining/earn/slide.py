#encoding=utf-8
#
#克服滑点的程序
#

import sys
sys.path.append("./")

from conf import *
import data
import wave
import loss
import bankManage

def slideTrade(f,conf=basicConf,arg=None):
	conf.dir=0
	d=data.Data()
	lastD=data.Data()
	tt=0
	askDiv=0
	bidDiv=0
	bank=bankManage.Bank() #仓位管理
	detail=(arg=="detail")
	start=conf.StartValue   #初始资产
	curve=loss.Loss(start)
	minFlow=curve.now
	flow=curve.now
	buyPos=-1  #建仓位置
	lastLossPos=0
	LSize=conf.LSize
	v0=0
	tni=0
	lastP=-1
	lastTT=0
	td=None
	conf.LargeSize=10*conf.Size
	phase=wave.Phase([conf.LargeSize,conf.Size])
	for line in f:
		tt+=1
		if tt < 2:
			continue
		try:
			lastTd,td=data.readTick(line,d,lastD,conf)
		except Exception,e:
			sys.stderr.write("readTick Fail!\n")
			sys.stderr.write(str(e)+"\n")
			continue
		if td == None:
			continue
		v0=td.p
		curve.addTime()
		phase.simpleAdd(v0)
		lavg=phase.avg(conf.LargeSize)
		avg=phase.avg(conf.Size)
		#if tt > conf.Size+2:
		#	print avg,lavg
		KD=(avg > lavg and askDiv >= conf.base*4 and askDiv <= conf.base*16 and bidDiv <= conf.base)
		KK=(lavg > avg  and askDiv <= conf.base and bidDiv >= conf.base*4 and bidDiv <= conf.base*16)
		#print tt,lastLossPos,LSize
		if abs(bank.bank) < 1e-6: #信号开仓
			askDiv=td.ask-td.p
			bidDiv=td.p-td.bid
			unit=int((curve.now*conf.GangRate*conf.BankRate)/(v0*conf.HandSize)) #按v0计算买入手数
			if tni <= 0 and KD and tt-lastLossPos > LSize:
				tni+=unit
				buyPos=tt #记录开仓位置
			if tni >= 0 and KK and tt-lastLossPos > LSize:
				tni=-unit
				buyPos=tt
		elif abs(tni) < 1e-6:
			e1=0
			if bank.bank > 0:
				e1=v0-bank.price
			else:
				e1=bank.price-v0
			if e1 < -(conf.MaxLoss*bank.price) or (bank.bank > 0 and KK) or (bank.bank < 0 and KD): #止损平仓
				tni=-bank.bank
				lastLossPos=tt
			elif tt - buyPos > conf.MidSize: #到指定周期结束平仓
				tni=-bank.bank
		if abs(tni) > 0 and lastP < 0:
			lastP=v0
			lastTT=tt
		if lastP > 0:
			if tni > 0:
				if tt - lastTT < conf.PTIME and (td.ask <= lastP or v0 < lastP):
					(e,f)=bank.addBank(tni,lastP,conf) 
					curve.addWin(e/conf.HandSize,conf) #计算胜率
					curve.addE(e)
					tni=0
					lastP=-1
				elif tt - lastTT >= conf.PTIME:
					#if bank.bank < -0.1:
					(e,f)=bank.addBank(tni,td.ask,conf)
					curve.addWin(e/conf.HandSize,conf) #计算胜率
					curve.addE(e)
					tni=0
					lastP=-1
			elif tni < 0:
				if tt - lastTT < conf.PTIME and (td.bid >= lastP or v0 > lastP):
					(e,f)=bank.addBank(tni,lastP,conf)
					curve.addWin(e/conf.HandSize,conf) #计算胜率
					curve.addE(e)
					tni=0
					lastP=-1
				elif tt - lastTT >= conf.PTIME:
					#if bank.bank > 0.1:
					(e,f)=bank.addBank(tni,td.bid,conf)
					curve.addWin(e/conf.HandSize,conf) #计算胜率
					curve.addE(e)
					tni=0
					lastP=-1
		else:
			curve.addTrackE((v0-bank.price)*bank.bank*conf.HandSize) #更新资产净值
	if td != None:
		if bank.bank > 0:
			tradePrice=td.bid
		else:
			tradePrice=td.ask
		(e,f)=bank.destroy(tradePrice,conf) #测试结束，强制平仓
		curve.addE(e) #更新资产净值
		if abs(e) > 0:
			curve.addWin(e/conf.HandSize,conf) #计算胜率
		flow+=f
	print curve.confStr(conf)
	return curve

def bi(f,conf=basicConf,arg=None):
	d=data.Data()
	lastD=data.Data()
	tt=0
	askDiv=0
	bidDiv=0
	bank=bankManage.Bank() #仓位管理
	detail=(arg=="detail")
	curve=loss.Loss(conf.StartValue)
	stage=0
	buyPrice=-1
	sellPrice=-1
	bn=0
	sn=0
	SRate=0.2
	Slide=0.1
	conf.LSize=20
	unit=0
	for line in f:
		tt+=1
		if tt < 2:
			continue
		try:
			lastTd,td=data.readTick(line,d,lastD,conf)
		except Exception,e:
			sys.stderr.write("readTick Fail!\n")
			sys.stderr.write(str(e)+"\n")
			continue
		if td == None:
			continue
		if stage == 0:
			askDiv=td.ask-td.p
			bidDiv=td.p-td.bid
			if askDiv >= conf.RR and bidDiv >= conf.RR:
				buyPrice=td.bid+conf.DD
				sellPrice=td.ask-conf.DD
				unit=int((curve.now*conf.GangRate*conf.BankRate)/((buyPrice+sellPrice)*conf.HandSize)) #按v0计算买入手数
				bn=0
				sn=0
				stage=1
				t0=tt
		elif stage == 1:
			if bn+sn == 2:
				stage=0
				bn=0
				sn=0
			elif tt - t0 > conf.MidSize:
				t1=tt
				if abs(bank.bank) > 0:
					(e,f)=bank.destroy(td.p+Slide,conf) #平单腿
					curve.addE(e)
					curve.addWin(e/conf.HandSize,conf)
					stage=2
					bn=0
					sn=0
				else:
					bn=0
					sn=0
					stage=0
			else:
				if (td.ask <= buyPrice or td.p < buyPrice):
					bn=1
					a=buyPrice
					if a > td.ask:
						a=td.ask
					if a > td.p+0.2:
						a=td.p+0.2
					a=buyPrice*(1.0-SRate)+SRate*a
					(e,f)=bank.addBank(unit,a,conf)
				if (td.bid >= sellPrice or td.p > sellPrice):
					sn=1
					a=sellPrice
					if a < td.bid:
						a=td.bid
					if a < td.p-0.2:
						a=td.p-0.2
					(e,f)=bank.addBank(-unit,a,conf)
		elif stage == 2:
			if tt - t1 > conf.LSize:
				stage=0
	if abs(bank.bank) > 0:
		(e,f)=bank.destroy(td.p+Slide,conf) #平单腿
		curve.addE(e)
		curve.addWin(e/conf.HandSize,conf)
	print curve
	return curve

def count(f,pt,mi,ma):
	d=data.Data()
	lastD=data.Data()
	tt=0
	lastTT=0
	lastP=-1
	cost=0
	cn=0
	cr=0
	PTIME=int(pt)
	lr=10000.0
	stype="Neg"
	stype="Phase"
	#stype="Pos"
	for line in f:
		try:
			lastTd,td=data.readTick(line,d,lastD,basicConf)
		except Exception,e:
			sys.stderr.write("readTick Fail!\n")
			sys.stderr.write(str(e)+"\n")
			continue
		if td == None:
			continue
		tt+=1
		if tt % 120 == 13 and lastP < 0:
			lastP=d.p
			lastBid=d.bid+mi
			lastBid1=lastBid
			lastBid2=lastBid
			High=False
			High1=False
			lastAsk=d.ask
			cn+=1
			lastTT=tt
		if lastP > 0:
			tdiv=tt - lastTT
			PLR=(d.ask - lastP >= lr)
			NLR=(lastP -d.bid >= lr)
			if stype == "Neg":
				if  tt - lastTT < PTIME and (d.bid >= lastP or d.p+ma >= lastP):
					lastP=-1
				elif tt - lastTT >= PTIME or PLR:
					cr+=1
					cost+=(lastP-d.bid)
					lastP=-1
			elif stype == "Pos":
				if tdiv < PTIME and (d.ask <= lastBid or d.p+ma <= lastBid):
					a=lastBid
					if a > d.ask:
						a=d.ask
					#if a > d.p+0.2:
					#	a=d.p+0.2
					if tdiv > 0:
						a=lastBid
					cost+=(a-lastP)
					lastP=-1
				elif tt - lastTT >= PTIME or NLR:
					cr+=1
					cost+=(d.ask-lastP)
					lastP=-1
			elif stype == "Phase":
				if tdiv==PTIME/3:
					lastBid1=d.bid+mi
					#if lastBid1 > d.p+add:
					#	lastBid1=d.p+add
				elif tdiv == (2*PTIME)/3:
					lastBid2=d.bid+mi
					#if lastBid2 > d.p+add:
					#	lastBid2=d.p+add
				if tdiv < PTIME/3 and (d.ask <= lastBid or d.p+ma <= lastBid):
					#if lastBid+0.01 < lastP:
					#	print "lastBid:%.2f\td.ask:%.2f\td.p:%.2f\tlastP:%.2f"%(lastBid,d.ask,d.p,lastP)
					a=lastBid
					if a > d.ask:
						a=d.ask
					#if a > d.p+0.2:
					#	a=d.p+0.2
					if tdiv > 0:
						a=lastBid
					cost+=(a-lastP)
					#print "tdiv:%d\ttt:%d\tlastBid:%.2f\tadd:%.3f\tcost:%.3f"%(tt-lastTT,lastBid,lastBid-lastP,cost)
					#print "b0",(lastBid-lastP)
					lastP=-1
				elif  tdiv < (2*PTIME)/3 and tdiv >= PTIME/3 and (d.ask <= lastBid1 or d.p+ma <= lastBid1):
					cr+=1
					a=lastBid1
					if a > d.ask:
						a=d.ask
					#if a > d.p+0.2:
					#	a=d.p+0.2
					if tdiv > PTIME/3:
						a=lastBid1
					cost+=(a-lastP)
					lastP=-1
				elif  tdiv < (3*PTIME)/3 and tdiv >= (2*PTIME)/3 and (d.ask <= lastBid2 or d.p+ma <= lastBid2):
					cr+=2
					a=lastBid2
					if a > d.ask:
						a=d.ask
					#if a > d.p+0.2:
					#	a=d.p+0.2
					if tdiv > (2*PTIME)/3:
						a=lastBid2
					cost+=(a-lastP)
					lastP=-1
				elif tdiv >= PTIME:
					cr+=3
					cost+=(d.ask-lastP)
					#print "a",(d.ask-lastP)
					lastP=-1
	#print "cost:%f\tcn:%d\tavg:%f\tcr:%d\tcrate:%.4f"%(cost,cn,cost/cn,cr,cr/float(cn))
	return (cost,cn,cr)
	
import os
def countFiles():
	#ts=[0,10,30,60,100]
	ts=[0,15,60,30]
	mins=[0.2,0.4]
	maxs=[0.0,0.2]
	#rs=[0.2,0.4,1.0,1.5,2.0,2.5,10.0,100.0]
	ds=os.listdir("./CFTICK")
	for di in ds:
		if len(di) < 6:
			continue
		try:
			d1=int(di[2:6])
		except:
			continue
		files=[]
		files=os.listdir("./CFTICK/"+di)
		if di == "20150521":
			print files
		for file in files:
			if not file.startswith("IF"):
				continue
			if len(file) < 6:
				continue
			try:
				d2=int(file[2:6])
			except:
				continue
			if d2-d1 < 2:
				continue
			for t in ts:
				for mi in mins:
					for ma in maxs:
						try:
							(cost,cn,cr)=count(open("./CFTICK/"+di+"/"+file),t,mi,ma)
							print "%s\t%s\t%d\t%.2f\t%.2f\t%f\t%d\t%f\t%d\t%.6f"%(di,file,t,mi,ma,cost,cn,cost/cn,cr,cr/float(cn))
						except Exception,e:
							sys.stderr.write("count and print Fail!\n")
							sys.stderr.write(str(e)+"\n")
							pass
						if t == 0:
							break
					if t == 0:
						break

def tradeFiles():
	ds=os.listdir("./CFTICK")
	conf=Conf()
	for di in ds:
		if len(di) < 6:
			continue
		d1=int(di[2:6])
		files=os.listdir("./CFTICK/"+di)
		for file in files:
			if not file.startswith("IF"):
				continue
			d2=int(file[2:6])
			if d2 -d1 >= 2:
				continue
			#for ml,base in [(0.0006,0.15),(0.0006,0.1),(0.0004,0.1),(0.0004,0.3),(0.001,0.2),(0.0006,0.2),(0.0008,0.1),(0.0004,0.2),(0.0002,0.2),(0.0004,0.09)]:
			#	for ms in [360,480,240]:
			#			for PTIME in [10,20,60,120]:
			for RR in [0.4,0.8,1.0,2.0]:
				for DD in [0.2,0.4,0.6]:
					if DD >= RR:
						continue
					for MidSize in [10,20,60]:
							#conf.PTIME=PTIME
							#conf.base=base
							#conf.MaxLoss=ml
							#conf.MidSize=ms
							#conf.LSize=ms
							conf.MidSize=MidSize
							conf.RR=RR
							conf.DD=DD
							#curve=slideTrade(open("./CFTICK/"+di+"/"+file),conf=conf)
							curve=bi(open("./CFTICK/"+di+"/"+file),conf=conf)
							add=(curve.now-curve.start)/curve.start
							#print "%s\t%s\t%d\t%.2f\t%.4f\t%d\t%d\t%.6f\t%.4f\t%.2f"%(di,file,conf.PTIME,conf.base,conf.MaxLoss,conf.MidSize,conf.LSize,add,curve.maxTrack,curve.maxLossTime)
							print "%s\t%s\t%.2f\t%.2f\t%d\t%.2f"%(di,file,RR,DD,MidSize,add)
def testCount():
	lr=1000.0
	if len(sys.argv) >= 3:
		lr=float(sys.argv[2])
	count(sys.argv[1],lr)

import random
def countCount(f=sys.stdin):
	cs={}
	for line in f:
		try:
			cols=line.strip().split()
			if random.random() > 1.3:
				continue
			k="%s\t%s\t%s"%(cols[2],cols[3],cols[4])
			#k="%s\t%s\t%s\t%s"%(cols[2],cols[3],cols[4],cols[5])
			v=float(cols[7])
			#k="%s\t%s"%(cols[2],cols[3])
			#v=float(cols[6])
			if k not in cs:
				cs[k]=[0,0]
			cs[k][0]+=v
			cs[k][1]+=1
		except Exception,e:
			sys.stderr.write(str(e)+"\n")
			continue
	for k in cs:
		print "%s\t%f\t%d\t%f"%(k,cs[k][0],cs[k][1],cs[k][0]/cs[k][1])

if __name__=="__main__":
	#testCount()
	if len(sys.argv) >= 2:
		countCount()
	else:
		#tradeFiles()
		countFiles()
	#countCount()


