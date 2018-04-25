#encoding=utf-8

import sys
import random
from conf import *
import wave
import loss
import bankManage
import data

sys.path.append("../util")
import Stack


def clear(tt,blist,v0,vm,conf,curve,detail):
	if blist.numOfE <= 0:
		return 0
	tn=0
	(start,end)=blist.eleIndexs()
	#print "start",start,"end",end,"numOfE",blist.numOfE
	for i in range(start,end,1):
		nn,price,adjust,ntt,msize=blist.vs[i]
		if tt - ntt < conf.Delay: #T+1
			break
		if abs(nn) <= 0:
			continue
		e=0
		e1=0
		divRate=1.0
		if nn > 0:
			e=v0-adjust
			e1=v0-price
			#if mavg > lavg:
			#	divRate=conf.divRate
		else:
			e=adjust-v0
			e1=price-v0
			#if mavg < lavg:
			#	divRate=conf.divRate
		if e > conf.AdjustRate*price:
			blist.vs[i][2]=(conf.MoveLossRate*v0+adjust*(1.0-conf.MoveLossRate))
			#print i,nn,v0,price,e,blist[i][2]
		if e < -(divRate*conf.MaxLoss*price) or e1 > (divRate*conf.MaxEarn*price):
			tn-=nn
			e=nn*(v0-price)
			curve.addWin(nn*(vm-price)-abs(nn)*(vm+price)*conf.Rate,conf)
			blist.vs[i][0]=0
			if detail:
				print "earn",e1 > (divRate*conf.MaxEarn*price),"nn:",nn,"tt",tt,"i",i,"price:",price,"adjust",adjust,"v0:",v0,"e",e,"ntt",ntt
			continue
	bvss=blist.vs[start]
	if tt - bvss[3] >= bvss[4]: #到指定周期
		#print tt,bvss[3],start,blist.numOfE
		if abs(bvss[0]) > 0:
			tn-=bvss[0]
			curve.addWin(bvss[0]*(vm-bvss[1])-abs(bvss[0])*(vm+bvss[1])*conf.Rate,conf)
			if detail:
				e=bvss[0]*(v0-bvss[1])
				print "n0:",bvss[0],bvss[1],v0,e
		#del blist[0]
		bvss[0]=0
		blist.minusIndex()
		#print "after minus:",blist.numOfE
	return tn

def smoothPrice(ptime,ti,conf=PricePhaseConf()):
	v=ptime[ti]
	for i in range(ti-conf.Div,ti+conf.Div+1,1):
		if ptime[ti] > v:
			v=ptime[ti]
	return v

def pricePhase(f,conf=PricePhaseConf(),arg=None):
	bank=bankManage.Bank()
	detail=(arg=="detail")
	start=conf.StartValue
	curve=loss.Loss(start)
	tt=0
	tn=0
	blist=Stack.Queue(conf.MidSize+1,[0,0,0,0,conf.MidSize])
	restBank=[0,0] #余量窗口
	minFlow=curve.now
	flow=curve.now
	lastDay=""
	day=""
	vm=0
	MaxPNum=1000000
	ptime=[0]*MaxPNum
	for line in f:
		tt+=1
		lastD,nowD=data.readLine(line,conf)
		if nowD == None:
			continue
		v1=nowD.p
		vpn=int(v1*5.0)
		vm=v1
		ptime[vpn]=tt
		#if detail:
		#	print "v1",v1,"vpn",vpn
		if arg == "Curve":
			print "%s%.4f"%(conf.CurveFix,curve.now/start*30.0)
		curve.addTime()
		clearN=clear(tt,blist,v1,vm,conf,curve,detail)
		tn=restBank[0] #-restBank[0] #与整数操作的差额操作
		tn+=clearN
		createN=0
		pt0=tt
		pt_1=smoothPrice(ptime,vpn-conf.SmallSize)
		pt1=smoothPrice(ptime,vpn+conf.SmallSize)
		pt_2=smoothPrice(ptime,vpn-conf.Size)
		pt2=smoothPrice(ptime,vpn+conf.Size)
		pt_3=smoothPrice(ptime,vpn-conf.LargeSize)
		pt3=smoothPrice(ptime,vpn+conf.LargeSize)
		#pt_4=smoothPrice(ptime,vpn-conf.MSize)
		#pt4=smoothPrice(ptime,vpn+conf.MSize)
		if data.afterInDay(nowD,conf.DayEnd) <= 0 and data.afterInDay(nowD,conf.DayStart) >= 0:
			#createN=create(tt,conf,phase,vm,curve,bank,detail)
			msize=conf.MidSize
			if pt0 > pt_2 and pt_1 > pt1 and pt0 > pt_2 and pt_2 > pt_3 and pt0-pt_3 < 200:
				msize=conf.MidSize
				createN=(curve.now/msize*conf.BankRate/vm*conf.GangRate/conf.HandSize)
				if detail:
					print "pt0",pt0,"pt1",pt1,"pt2",pt2,"pt3",pt3,
					print "pt0",pt0,"pt_1",pt_1,"pt_2",pt_2,"pt_3",pt_3,"msize",msize,"v1",v1,"cn",createN
			if pt0 > pt2 and pt1 > pt_1 and pt0 > pt2 and pt2 > pt3 and pt0-pt3 < 200:
				msize=conf.MidSize
				createN=-(curve.now/msize*conf.BankRate/vm*conf.GangRate/conf.HandSize)
				if detail:
					print "pt0",pt0,"pt_1",pt_1,"pt_2",pt_2,"pt_3",pt_3
					print "pt0",pt0,"pt1",pt1,"pt2",pt2,"pt3",pt3,"msize",msize,"v1",v1,"cn",createN
			if createN > 0 and createN < conf.unitLowRate*conf.UnitLimit and bank.bank <= 0:
				createN=conf.UnitLimit
			if createN < 0 and createN > -conf.unitLowRate*conf.UnitLimit and bank.bank >= 0:
				createN=-conf.UnitLimit
			#afterWardFac=afterWardFac*(conf.MidSize-1.0)/conf.MidSize+createN/conf.MidSize
			#if abs(afterWardFac) > 0.8*abs(createN):
			#	createN+=0.02*afterWardFac
			tryBank=bank.bank+tn
			ceilBank=(curve.now*conf.GangRate*conf.BankRate)/(v1*conf.HandSize)
			if abs(createN) > 0 and abs(createN+tryBank) > ceilBank:
				#if random.random() > 0.9:
				#	print "createN",createN,"tryBank",tryBank,"ceilBank",ceilBank
				createN*=(ceilBank-abs(tryBank))/abs(createN)
			tn+=createN
			if createN > 0:
				blist.add([createN,vm+conf.SlideRate,vm+conf.SlideRate,tt,msize])
			if createN < 0:
				blist.add([createN,vm-conf.SlideRate,vm-conf.SlideRate,tt,msize])
		tni=int(tn)	
		tnv=tn-int(tn) #int(tn)-tn
		if tnv > conf.StopLossNearInt and bank.bank+tni < 0:
			tni+=1
			tnv-=1.0
		if tnv < -conf.StopLossNearInt and bank.bank+tni > 0:
			tni-=1
			tnv+=1.0
		if conf.Stock and  (tni+bank.bank) < 0:
			tni=-bank.bank
			tnv=tn-tni #tni-tn
		if abs(tni) > 0:
			bb=bank.bank
			bp=bank.price
			bf=flow
			if tni > 0:
				vm=vm+conf.SlideRate
			else:
				vm=vm-conf.SlideRate
			(e,f)=bank.addBank(tni,vm,conf)
			curve.addE(e)
			flow+=f
			if detail:
				print "tt:",tt,"bb:",bb,"bp:",bp,"tni:",tni,"vm:",vm,"now:",curve.now,"e:",e,"f:",f,"flow:",flow
			dflow=curve.now-bank.value(conf)/conf.GangRate-flow
			if abs(dflow) > 1.0:
				print line
				print dflow,curve.now,tni,"vm:",vm,"bb:",bb,"bp:",bp,"after:",bank,e,bf,f,flow
			if flow < minFlow:
				minFlow=flow
		else:
			#浮动
			#pass
			curve.addTrackE((vm-bank.price)*bank.bank*conf.HandSize)
		restBank=[tnv,vm]
	if detail:
		print "destroy: vm",vm,"bank",bank.bank,"price",bank.price,"now",curve.now
	(e,f)=bank.destroy(vm,conf)
	curve.addE(e)
	flow+=f
	if detail:
		print "after destroy:bank",bank.bank,"e",e,"flow",flow,"now",curve.now
	if abs(e) > 0:
		curve.addWin(e/conf.HandSize,conf)
	print curve.confStr(conf)	
	print minFlow

	

if __name__=="__main__":
	#pricePhase(sys.stdin,PricePhaseConf(),arg=sys.argv[1])
	arg=None
	if len(sys.argv) >= 2:
		arg=sys.argv[1]
	pricePhase(sys.stdin,conf=PricePhaseConf(),arg=arg)


