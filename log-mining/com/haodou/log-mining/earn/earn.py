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

def readTags(file):
	tags=[]
	for line in open(file):
		t=line.strip().split()[0]
		tags.append(t)
	return tags

def updateEs(es,e):
	if len(es) <= 0:
		return
	for i in range(0,len(es)-1,1):
		es[i]=es[i+1]
	if e > 0:
		es[-1]=True
	else:
		es[-1]=False

def getER(weit,predict,i,conf=basicConf):
	ct=float(predict[i])
	if ct > conf.NoBidFold*conf.Rate/conf.GangRate:
		ct=1.0
	elif ct < -conf.NoBidFold*conf.Rate/conf.GangRate:
		ct=-1.0
	else:
		ct=0
	return ct*float(weit[i])

def checkNoBid(predict,i,conf=basicConf):
	ct=float(predict[i])
	if ct >= -conf.NoBidFold*conf.Rate/conf.GangRate and ct <= conf.NoBidFold*conf.Rate/conf.GangRate:
		return True
	return False


def count(conf=basicConf):
	weit=readTags(sys.argv[2])
	predict=readTags(sys.argv[3])
	randseed=random.random()*(1.0-conf.SampleRate)
	print "randseed:%.6f\ts:%d\te:%d"%(randseed,int(randseed*len(weit)),(randseed+conf.SampleRate)*len(weit))
	earn=0
	lost=0
	en=0
	ln=0
	qn=0
	d={}
	n=conf.MidSize
	tt=0
	noBid=0
	adjustNum=0
	bank=0
	banks=[0,0,0]
	PhaseEQ=0
	PhaseNE=0
	vs=[]
	curve=loss.Loss(float(n))
	for i in range(len(weit)):
		ir=float(i)/len(weit)
		if ir < randseed or ir >=randseed+conf.SampleRate:	
			continue
		curve.addTime()
		print "%s%.4f"%(conf.CurveFix,curve.now)
		if checkNoBid(predict,i,conf):
			noBid+=1
			continue
		pi=int(predict[i])
		tt+=1
		er=getER(weit,predict,i,conf)
		e=er*curve.now/n*conf.BankRate*conf.GangRate
		#print weit[i],predict[i],now/n,now,e
		fee=curve.now/n*conf.BankRate*conf.Rate
		if i >= conf.MidSize:
			if predict[i] == predict[i-conf.MidSize]: #如果方向相同，则不用动作
		
				ef=e
				PhaseEQ+=1
			else:
				ef=e-2*fee
				PhaseNE+=1
		else:
			ef=e-fee
		if True:
			if e > 0:
				earn+=ef
				en+=1
			elif e < 0:
				#print "hi"
				lost+=ef
				ln+=1
			else:
				qn+=1
				lost+=ef
			curve.addE(ef)
			#print now/n,er,e,fee,ef

	print "earn:%.4f\t%.4f"%(earn,lost)
	print "准确率：正确=%d\t错误=%d\t平衡=%d\t准确率=%.4f\t正率=%.4f\tnoBid=%d"%(en,ln,qn,float(en)/(en+ln+1e-2),float(en)/(en+ln+qn+1e-2),noBid)
	#print "PhaseEQ:%d\tPhaseNE:%d\tbanks:%s"%(PhaseEQ,PhaseNE,str(banks))
	print curve

def direction(phase,conf,size,detail=False,):
	phase.updateSize()
	#a1=phase.a1(size)
	avg=phase.avg(size)
	v0=phase.v0()
	if v0 == None:
		return 0
	max=phase.max(size)
	min=phase.min(size)
	#(r,mr)=phase.base.rangeV(20,10)
	#if detail:
	#	print "a1",a1
	#if size == conf.Size:
	#	print "%.4f\t%.4f"%(a1,phase.divAvg(size))
	#if a1 < conf.a1Rate*v0 and a1 > -conf.a1Rate*v0:
	#	return 0
	#print a1,v0,phase.divAvg(size)
	#if abs(a1)*v0 < conf.a1DivRate*phase.divAvg(size):
		#print a1,v0,phase.divAvg(size)
		#return 0
	a1r=1.0
	#if abs(a1) < conf.a1Rate*v0:
	#	a1r=conf.a1Discount
	#mmin=phase.min(20)
	#mmax=phase.max(20)
	savg=phase.avg(int(size/conf.SmallDiv))
	smax=phase.max(int(size/conf.SmallDiv))
	smin=phase.min(int(size/conf.SmallDiv))
	lavg=phase.avg(int(size*conf.LargeDiv))
	lmax=phase.max(int(size*conf.LargeDiv))
	lmin=phase.min(int(size*conf.LargeDiv))
	if detail:
		print "v0:",v0,"savg:",savg,"avg:",avg,"lavg:",lavg
	if conf.DType=="bulin":
		#sys.stderr.write("bulin\n")
		#print "v0:%.2f"%(v0),"savg: %.2f"%(savg),"div:%.2f"%(conf.TickDiv),"avg:%.2f"%(avg),"lavg:%.2f"%(lavg)
		if v0 > savg and savg > avg*(1.0 - 0.5*conf.TickDiv) and savg < avg*(1.0 + 3*conf.TickDiv) and avg > lavg*(1.0-conf.LongDiv):
			return a1r
		if v0 < savg and savg < avg*(1.0 + 0.5*conf.TickDiv) and savg > avg*(1.0 - 3*conf.TickDiv) and avg < lavg*(1.0+conf.LongDiv):
			return -a1r
	else:
		if v0 > savg and savg > avg and avg > lavg:
			return a1r
		if v0 < savg and savg < avg and avg < lavg:
			return -a1r
	return 0

def clear(tt,blist,v0,vm,conf,curve,phase,detail):
	if blist.numOfE <= 0:
		return 0
	size=conf.MidSize
	tn=0
	#lavg=phase.avg(int(conf.Size*conf.LargeDiv))
	#mavg=phase.avg(int(conf.Size*conf.LargeDiv*conf.MDiv))
	(start,end)=blist.eleIndexs()
	#print "start",start,"end",end,"numOfE",blist.numOfE
	for i in range(start,end,1):
		nn,price,adjust,ntt=blist.vs[i]
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
				print "nn:",nn,"tt",tt,"i",i,"price:",price,"adjust",adjust,"v0:",v0,"e",e,"ntt",ntt
			continue
	bvss=blist.vs[start]
	if tt - bvss[3] >= size:
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

def create(tt,conf,phase,vm,curve,bank,detail):
	#if bank.bank == 0 and tt-conf.TPos >= 10:
	n=0
	if tt % conf.TSize == conf.TPos%conf.TSize:
		v0=phase.v0()
		d=direction(phase,conf,conf.Size,detail)
		#print v0,d
		if d != 0:
			#if (bank.bank == 0 and (tt-conf.TPos)%conf.TSize > 10):
			#	conf.TPos=tt%conf.TSize
			#conf.TPos=tt
			n=0
			if v0 > 0:
				n=(d*curve.now*conf.GangRate*conf.BankRate)/(conf.TT*v0*conf.HandSize)
				#n=(d*curve.now/conf.MidSize*conf.TSize*conf.BankRate/v0*conf.GangRate/conf.HandSize)
				#if random.random() > 0.99:
				#	print "n",n,"n11",n11,"div",n-n11
				if conf.Only:
					n=int(n)
			#if abs(n) < (1.1*conf.TSize)/(conf.MidSize):
			#	if detail:
			#		print "too small n",n,"now",curve.now,"d",d
			#	n=0.0
			if conf.Stock:
				if n + tn + bank.bank< 0:
					if detail:
						print "sctok-before:n",n,"tn",tn,"bank.bank",bank.bank
					n=-(tn+bank.bank)
					if detail:
						print "sctok-after:n",n,"tn",tn,"bank.bank",bank.bank
			if detail:
				print "d",d,"tt",tt,"n:",n
			#tn+=n
			#blist.add([n,vm,vm,tt])
	return n

'''
def checkBank(blist,bankDiff,conf,detail):
	if blist.numOfE <= 0:
		return 0
	size=conf.MidSize
	(start,end)=blist.eleIndexs()
	diff=bankDiff
	if abs(diff) > 1e-6:
		if detail:
			print "nn(b0)",blist.vs[start][0],"bankDiff",bankDiff,"diff",diff
		blist.vs[start][0]-=diff
		diff-=diff
		if detail:
			print "nn(0)",blist.vs[start][0],"bankDiff",bankDiff,"diff",diff
	return bankDiff
'''

def phase(f,conf=basicConf,arg=None):
	bank=bankManage.Bank()
	detail=(arg=="detail")
	start=conf.StartValue
	curve=loss.Loss(start)
	tt=0
	tn=0
	phase=wave.Phase(conf.getSizes())
	volSize=30
	smallVolSize=10
	volPhase=wave.Phase([smallVolSize,volSize],[10,100])
	#vol2=wave.Phase([smallVolSize,volSize],[10,100])
	sum=0
	blist=Stack.Queue(conf.MidSize+1,[0,0,0,0])
	restBank=[0,0] #余量窗口
	minFlow=curve.now
	flow=curve.now
	lastDay=""
	day=""
	vm=0
	lastV=-1
	maxBuyRate=-1
	divs=[]
	divSum=0
	divN=0
	drs=[]
	drSum=0
	afterWardFac=0 #滞后因子
	myBank=0
	vbn=0
	for line in f:
		tt+=1
		lastD,nowD=data.readLine(line,conf)
		if nowD == None:
			continue
		v0=nowD.lastP
		if v0 <= 1e-12:
			sys.stderr.write(line)
			break
		v1=nowD.p
		lastV=v0
		vol=lastD.vol
		if lastD.vol > 0 and nowD.vol - lastD.vol< 5 and nowD.vol - lastD.vol >= 0 and nowD.vol < 3000:
			vbn+=1
		if vbn > 10:
			break
		if arg == "Curve":
			print "%s%.4f"%(conf.CurveFix,curve.now/start*30.0)
		#phase.add(v1)
		divs.append(v1)
		if len(divs) >= conf.DivSize:
			vopen=divs[-conf.DivSize]
			max=divs[-1]
			divRate=abs(max-vopen)/vopen
			drs.append(divRate)
			divSum+=abs(max-vopen)/vopen
			divN+=1
			#print "",conf.MaxLoss,"divSum",divSum,"divN",divN,divSum/divN,"max-vopen",max-vopen,(max-vopen)/vopen,"max",max,"vopen",vopen
			divs=[]
			drSum+=divRate
			if len(drs) > 100:
				drSum-=drs[0]
				del drs[0]
			#conf.MaxLoss=divSum/divN
			#conf.MaxLoss=0.99*divSum/divN+0.01*divRate
			#conf.MaxLoss=0.94*divSum/divN+0.05*drSum/len(drs)+0.01*divRate
		volPhase.simpleAdd(vol)
		phase.add(v1)
		#vol2.add(vol)
		curve.addTime()
		vm=v1
		#v0*(1.0-conf.SlideRate)+v1*conf.SlideRate
		#print "vm",vm
		if nowD.settle:
			if detail:
				print "destroy: vm",vm,"bank",bank.bank,"price",bank.price,"now",curve.now
			myBank=0
			e,f=bank.destroy(vm,conf)
			curve.addE(e)
			flow+=f
			if detail:
				print "after destroy:bank",bank.bank,"e",e,"flow",flow,"now",curve.now
			if abs(e) > 0:
				curve.addWin(e/conf.HandSize,conf)
			restBank=[0,vm]
			blist=Stack.Queue(conf.MidSize/conf.TSize+1,[0,0,0,0])
			continue
		bankDiff=myBank-bank.bank
		if abs(bankDiff) > 1e-6:
			if True:
				print "tt",tt,"tn",tn,"myBank",myBank,"bank",bank.bank,"rest",restBank[0],"bankDiff",bankDiff
			#myBank-=checkBank(blist,bankDiff,conf,detail)
		clearN=clear(tt,blist,v0,vm,conf,curve,phase,detail)
		tn=restBank[0] #-restBank[0] #与整数操作的差额操作
		tn+=bankDiff
		myBank-=bankDiff
		tn+=clearN
		createN=0
		if data.afterInDay(nowD,conf.DayEnd) <= 0 and data.afterInDay(nowD,conf.DayStart) >= 0:
			createN=create(tt,conf,phase,vm,curve,bank,detail)
			if createN > 0 and createN < conf.unitLowRate*conf.UnitLimit and bank.bank <= 0:
				createN=conf.UnitLimit
			if createN < 0 and createN > -conf.unitLowRate*conf.UnitLimit and bank.bank >= 0:
				createN=-conf.UnitLimit
			#afterWardFac=afterWardFac*(conf.MidSize-1.0)/conf.MidSize+createN/conf.MidSize
			#if abs(afterWardFac) > 0.8*abs(createN):
			#	createN+=0.02*afterWardFac
			tryBank=bank.bank+tn
			ceilBank=(curve.now*conf.GangRate*conf.BankRate)/(v0*conf.HandSize)
			if abs(createN) > 0 and abs(createN+tryBank) > ceilBank:
				#if random.random() > 0.9:
				#	print "createN",createN,"tryBank",tryBank,"ceilBank",ceilBank
				createN*=(ceilBank-abs(tryBank))/abs(createN)
			tn+=createN
			if abs(createN) > 0:
				blist.add([createN,vm,vm,tt])
		#phase.add(v1)
		#(tni,tnv)=near(tn)
		#tni=int(tn)+clearN
		tni=int(tn)	
		tnv=tn-int(tn) #int(tn)-tn
		if tnv > conf.StopLossNearInt and bank.bank+tni < 0:
			tni+=1
			tnv-=1.0
		if tnv < -conf.StopLossNearInt and bank.bank+tni > 0:
			tni-=1
			tnv+=1.0
		myBank+=tni
		#if abs(createN) > 0:
		#	blist.add([createN,vm,vm,tt])
		#if abs(int(tn)) > 0:
		#	blist.add([int(tn),vm,vm,tt])
		nfold=abs(tni*v1*conf.HandSize)/ (curve.now*conf.GangRate)
		if maxBuyRate < nfold:
			#print "nfold",nfold,"maxBuyRate",maxBuyRate,"curve.now",curve.now
			maxBuyRate=nfold
		#if nfold > 0.3:
			#print "nfold",nfold,"maxBuyRate",maxBuyRate,"curve.now",curve.now
		if conf.Stock and  (tni+bank.bank) < 0:
			tni=-bank.bank
			tnv=tn-tni #tni-tn
		if abs(tni) > 0:
			if tni > 0:
				vm=vm*(1.0+conf.SlideRate/2500.0)
			else:
				vm=vm*(1.0-conf.SlideRate/2500.0)
			bb=bank.bank
			bp=bank.price
			bf=flow
			if random.random() < 1.999: #0.1%失败交易
				(e,f)=bank.addBank(tni,vm,conf)
				curve.addE(e)
				flow+=f
				if detail:
					print "tt:",tt,"bb:",bb,"bp:",bp,"tni:",tni,"vm:",vm,"now:",curve.now,"e:",e,"f:",f,"flow:",flow
			else:
				print "trade fail: tni",tni
			dflow=curve.now-bank.value(conf)/conf.GangRate-flow
			if abs(dflow) > 1.0:
				print line
				print dflow,curve.now,tni,"vm:",vm,"bb:",bb,"bp:",bp,"after:",bank,e,bf,f,flow
				break
			if flow < minFlow:
				minFlow=flow
			#浮动
			#pass
		curve.addTrackE((vm-bank.price)*bank.bank*conf.HandSize)
		if arg == "Bank":
			#print "%s%.4f"%(conf.CurveFix,tni)
			print "%s%.4f"%(conf.CurveFix,flow/curve.now)
		if conf.Stock and bank.bank < 0:
			print "bank<0",curve.now,bank,tnv
		restBank=[tnv,vm]
	if detail:
		print "destroy: vm",vm,"bank",bank.bank,"price",bank.price,"now",curve.now
	if bank.bank > 0:
		vm=vm*(1.0-conf.SlideRate/2500.0)
	else:
		vm=vm*(1.0+conf.SlideRate/2500.0)
	(e,f)=bank.destroy(vm,conf)
	curve.addE(e)
	flow+=f
	if detail:
		print "after destroy:bank",bank.bank,"e",e,"flow",flow,"now",curve.now
	if abs(e) > 0:
		curve.addWin(e/conf.HandSize,conf)
	return curve
	#confStr(conf)	
	#print minFlow

def m2(f,conf1=MultiConf(1),conf2=MultiConf(2),arg=None):
	detail=(arg=="detail")
	bank1=bankManage.Bank()
	bank2=bankManage.Bank()
	start=conf1.StartValue
	curve=loss.Loss(start)
	tt=0
	phase1=wave.Phase(conf1.getSizes())
	blist1=Stack.Queue(conf1.MidSize/conf1.TSize+1,[0,0,0,0])
	phase2=wave.Phase(conf2.getSizes())
	blist2=Stack.Queue(conf2.MidSize/conf2.TSize+1,[0,0,0,0])
	restBank1=[0,0] #余量窗口
	restBank2=[0,0]
	minFlow=curve.now
	flow=curve.now
	vm1=0
	vm2=0
	lastD1=data.Data()
	lastD2=data.Data()
	nowD1=data.Data()
	nowD2=data.Data()
	for line in f:
		tt+=1
		if arg == "Curve":
			print "%s%.4f"%(conf1.CurveFix,curve.now/start*30.0)
		if tt < 3: #忽略第一行
			continue
		lastD1,nowD1=data.readLine(line,conf1,nowD1,lastD1)
		lastD2,nowD2=data.readLine(line,conf2,nowD2,lastD2)
		if nowD1 == None or nowD2 == None:
			continue
		v01=nowD1.lastP
		v02=nowD2.lastP
		vm1=v01*(1.0-conf1.SlideRate)+conf1.SlideRate*nowD1.p
		vm2=v02*(1.0-conf2.SlideRate)+conf2.SlideRate*nowD2.p
		phase1.add(v01)
		phase2.add(v02)
		curve.addTime()
		if nowD1.settle:
			e,f=bank1.destroy(vm1)
			curve.addE(e)
			flow+=f
			restBank1=[0,vm1]
			blist1=Stack.Queue(conf1.MidSize/conf1.TSize+1,[0,0,0,0])
		if nowD2.settle:
			e,f=bank2.destroy(vm2)
			curve.addE(e)
			flow+=f
			restBank2=[0,vm2]
			blist2=Stack.Queue(conf2.MidSize/conf2.TSize+1,[0,0,0,0])
			continue
		tn1=clear(tt,blist1,v01,vm1,conf1,curve,phase1,detail)
		tn2=clear(tt,blist2,v02,vm2,conf2,curve,phase2,detail)
		tn1-=restBank1[0] #与整数操作的差额操作
		tn2-=restBank2[0]
		if tt % conf1.TSize == 7%conf1.TSize:
			d1=direction(phase1,conf1,conf1.Size,detail)
			d2=direction(phase2,conf2,conf2.Size,detail)
			br1=conf1.BankRate
			br2=conf2.BankRate
			#if abs(d2) >= abs(d1):
			#	br2+=br1
			#	br1=0
			if abs(d1) > 0:
				n1=(d1*curve.now/conf1.MidSize*conf1.TSize*br1/v01*conf1.GangRate/conf1.HandSize)
				tn1+=n1
				blist1.add([n1,vm1,vm1,tt])
			if abs(d2) > 0:
				n2=(d2*curve.now/conf2.MidSize*conf2.TSize*br2/v02*conf2.GangRate/conf2.HandSize)
				tn2+=n2
				blist2.add([n2,vm2,vm2,tt])
		(tni1,tnv1)=near(tn1)
		(tni2,tnv2)=near(tn2)
		if abs(tni1) > 0:
			(e,f)=bank1.addBank(tni1,vm1,conf1)
			curve.addE(e)
			flow+=f
		if abs(tni2) > 0:
			e,f=bank2.addBank(tni2,vm2,conf2)
			curve.addE(e)
			flow+=f
		if flow < minFlow:
			minFlow=flow
		restBank1=[tnv1,vm1]	
		restBank2=[tnv2,vm2]
	(e,f)=bank1.destroy(vm1)
	curve.addE(e)
	flow+=f
	(e,f)=bank2.destroy(vm2)
	curve.addE(e)
	flow+=f
	if flow < minFlow:
		minFlow=flow
	if arg == "Curve":
		print "%s%.4f"%(conf1.CurveFix,curve.now/start*30.0)
	print curve
	print minFlow

def arbitrage(f,conf1=MultiConf(1),conf2=MultiConf(2),arg=None):
	detail=(arg=="detail")


import os
def testPhase():
	dir="./data"
	files=os.listdir(dir)
	for file in files:
		print file
		conf=Conf()
		conf.DataType="stock"
		phase(open(dir+"/"+file),conf)

def testStockDay():
	dir="./stock_day"
	files=os.listdir(dir)
	for file in files:
		print file
		phase(open(dir+"/"+file),StockDayConf())

def testMulti():
	dir="/home/zhangzhonghui/libsvm/data/data/data/"
	files=os.listdir(dir)
	for file in files:
		if not file.startswith("if"):
			continue
		print file
		phase(open(dir+"/"+file))

def testDCE():
	import tt
	dir="/home/zhangzhonghui/log-mining/com/haodou/log-mining/earn/dce/DCE"
	#dir="./modiData"
	files=getFiles(dir)
	lastItem=""
	tn=0
	for file in files:
		day=file[len(dir)+1:len(dir)+9]
		vs=tt.t9(day,file)
		#print file
		#print phase(vs,DCEConf())
		p1=file.rfind("/")
		#p0=len(dir)+1
		#day=file[p0:p1]
		month=file[-8:-4]
		name=file[p1+1:-8]
		#curve=phase(vs,DCEConf())
		item=name+"\t"+day
		if item != lastItem:
			tn=0
		else:
			tn+=1
		lastItem=item
		#if len(vs) <  220:
		#	continue
		curve=phase(vs,DCEConf())
		#if curve.time < 100:
		#	continue
		#print file
		#print curve.confStr(DCEConf())
		print name+"\t"+day+"\t"+month+"\t"+str(curve.now/curve.start-1.0)+"\t"+str(tn)+"\t"+str(curve.win+curve.loss)

def testModi():
	dir="./modiData"
	files=getFiles(dir)
	for file in files:
		p=file.rfind("/")
		name=file[p+1:-8]
		month=file[-8:-4]
		curve=phase(open(file),DCEConf())
		print name+"\t"+month+"\t"+str(curve.now/curve.start-1.0)+"\t"+str(curve.win+curve.loss)

def testMulti1(arg):
	phase(sys.stdin,MultiConf(),arg)

def testMulti2(arg):
	phase(sys.stdin,MultiConf(2),arg)

def testTick():
	files=getFiles("/home/zhangzhonghui/libsvm/data/CFFEX/CFFEX")
	files=getFiles("/home/zhangzhonghui/log-mining/com/haodou/log-mining/earn/CFTICK")
	for file in files:
		if file.find("IF") < 0:
			continue
		#p=file.find("2015")
		#if p < 0:
		#	continue
		#s=file[p:p+6]
		#if s < "201503":
		#	sys.stderr.write(s+"\n")
		#	continue
		print file
		phase(open(file),TickConf())

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "count":
			count()
		elif sys.argv[1] == "div":
			div()
		elif sys.argv[1] == "stocks":
			testPhase()
		elif sys.argv[1] == "stockDay":
			testStockDay()
		elif sys.argv[1] == "multi":
			testMulti()
		elif sys.argv[1] == "dce":
			testDCE()
		elif sys.argv[1] == "dceSingle":
			print "dce single!!"
			conf=DCEConf()
			curve=phase(sys.stdin,conf=conf)
			print curve.confStr(conf)
		elif sys.argv[1] == "modi":
			testModi()
		elif sys.argv[1] == "multi1":
			arg=None
			if len(sys.argv) >= 3:
				arg=sys.argv[2]
			testMulti1(arg)
		elif sys.argv[1] == "multi2":
			arg=None
			if len(sys.argv) >= 3:
				arg=sys.argv[2]
			testMulti2(arg)
		elif sys.argv[1] == "ticks":
			testTick()
		elif sys.argv[1] == "tick":
			arg=None
			if len(sys.argv) >= 3:
				arg=sys.argv[2]
			phase(sys.stdin,TickConf(),arg)
		
		elif sys.argv[1] == "m2":
			conf1=MultiConf(1)
			conf1.BankRate=0.8
			conf2=MultiConf(2)
			conf2.BankRate=0.0
			arg=None
			if len(sys.argv) >= 3:
				arg=sys.argv[2]
			m2(sys.stdin,conf1,conf2,arg)
		else:
			if sys.argv[1] != "day":
				#print basicConf.DataType
				phase(sys.stdin,basicConf,sys.argv[1])
			else:
				MidSize=480
				arg=None
				if len(sys.argv) >= 3:
					try:
						MidSize=int(sys.argv[2])
						if len(sys.argv) >= 4:
							arg=sys.argv[3]
					except:
						arg=sys.argv[2]
				phase(sys.stdin,DaySizeConf(MidSize),arg)
	else:
		#print basicConf.DataType
		curve=phase(sys.stdin)
		print curve.confStr(basicConf)

