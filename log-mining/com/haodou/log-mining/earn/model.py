#encoding=utf-8

import sys
from conf import *
import loss
import earn
import wave
import bankManage
import data

sys.path.append("../util")
import Stack

class Model(object):
	def __init__(self,conf=basicConf):
		self.conf=conf
		self.blist=Stack.Queue(conf.MidSize/conf.TSize+1)

	def add(self,tt,vm,phase,curve,bank,detail):
		v0=phase.v0()
		tn=earn.clear(tt,self.blist,v0,vm,self.conf,curve,phase,detail)	
		tn=earn.create(tt,self.conf,phase,vm,self.blist,curve,tn,bank,detail)
		return tn

def getV(line,conf):
	lastD,nowD=data.readLine(line,conf)
	v0=nowD.lastP
	v1=nowD.p
	vm=v0*(1.0-conf.SlideRate)+v1*conf.SlideRate
	return (v0,vm,nowD.settle)

	'''
	cols=line.strip().split()
	v0=float(cols[0])
	if len(cols) >= 2:
		v1=float(cols[1])
	else:
		v1=v0
	vm=v0*(1.0-conf.SlideRate)+v1*conf.SlideRate
	return (v0,vm)
	'''

def modelCombinePhase(models,f,phase,curve,bank,tt,destroy=False,restBank=0,conf=basicConf,arg=None):
	detail=(arg=="detail")
	minFlow=curve.now
	flow=curve.now
	for line in f:
		(v0,vm,settle)=getV(line,conf)
		if settle:
			e,f=bank.destroy(vm)
			curve.addE(e)
			flow+=f
			if flow < minFlow:
				minFlow=flow
			continue
		phase.add(v0)
		if arg == "Curve":
			print "%s%.4f"%(conf.CurveFix,curve.now/conf.StartValue*30.0)
		curve.addTime()
		tn=-restBank
		for model,weit in models:
			tn+=weit*model.add(tt,vm,phase,curve,bank,detail)
		(tni,tnv)=near(tn)
		if conf.Stock and  (tni+bank.bank) < 0:
			tni=-bank.bank
			tnv=tni-tn
		if abs(tni) > 0:
			(e,f)=bank.addBank(tni,vm,conf)
			curve.addE(e)
			flow+=f
			if flow < minFlow:
				minFlow=flow
		restBank=tnv
		tt+=1
	if destroy:
		bank.destroy(vm)
	#print curve
	#print minFlow
	return (phase,curve,bank,tt,restBank)

WK=5
m2=[(1.0,0.0,wave.Phase([WK])),(0.0,1.0,wave.Phase([WK])),(0.5,0.5,wave.Phase([WK])),[0.6,0.4,wave.Phase([WK])]]

def selectModelCombine(models,vs,phase,curve,bank,tt,restBank,conf,arg,N):
	if len(models) == 2:
		MR=N/(100000.0+N)
		bw1=models[0][1]
		bw2=models[1][1]
		m2[-1][0]=bw1
		m2[-1][1]=bw2
		bmax=-1000000
		#(curve.now-baseRate)/(curve.maxTrack+baseRate*0.1)
		for w1,w2,wphase in m2:
			models[0][1]=w1
			models[1][1]=w2
			phase1=copy.deepcopy(phase)
			curve1=loss.Loss(curve.now)
			bank1=copy.deepcopy(bank)
			tt1=tt
			restBank1=restBank
			modelCombinePhase(models,vs,phase1,curve1,bank1,tt1,False,restBank1,conf,arg)
			wphase.add(curve1.now/curve1.start-1.0)
			bnow=wphase.avg(WK)-wphase.dev(WK)
			print "w1",w1,"w2",w2,"avg",wphase.avg(WK),"dev",wphase.dev(WK),"bnow",bnow,"bmax",bmax,"rate",curve1.now/curve1.start-1.0,"track",curve1.maxTrack           
			if bnow > bmax:
				print "w1",w1,"w2",w2,"bnow",bnow,"bmax",bmax
				bw1=w1
				bw2=w2
				bmax=bnow
		models[0][1]=bw1*MR+m2[-1][0]*(1.0-MR)
		models[1][1]=bw2*MR+m2[-1][1]*(1.0-MR)
		print "bw12",bw1,bw2
		print "best models:",models[0][1],models[1][1]
	return models

def modelCombine(models,f,conf=basicConf,arg=None):
	bank=bankManage.Bank()
	curve=loss.Loss(conf.StartValue)
	sizes=[]
	for model,weit in models:
		sizes.extend(model.conf.getSizes())
	phase=wave.Phase(sizes)
	tt=0
	vs=[]
	N=5000
	restBank=0 #余量窗口
	bw1=0.6
	bw2=0.4
	lastLen=0
	for line in f:
		vs.append(line)
		if len(vs) >= N:
			phase1=copy.deepcopy(phase)
			curve1=loss.Loss(curve.now)
			bank1=copy.deepcopy(bank)
			tt1=tt
			(phase,curve,bank,tt,restBank)=modelCombinePhase(models,vs[lastLen:],phase,curve,bank,tt,False,restBank,conf,arg)
			#models=selectModelCombine(models,vs,phase1,curve1,bank1,tt1,restBank,conf,arg,N)
			#print "model",models[0][1],models[1][1]
			#del vs[0:30000]
			vs=[]
			lastLen=len(vs)
	if len(vs) > 0:
		(phase,curve,bank,tt,restBank)=modelCombinePhase(models,vs[lastLen:],phase,curve,bank,tt,True,restBank,conf,arg)
	print curve

def test():
	models=[]
	models.append([Model(basicConf),1.0])
	models.append([Model(NoA1Conf()),0.0])
	arg=None
	if len(sys.argv) >= 2:
		arg=sys.argv[1]
	modelCombine(models,sys.stdin,arg=arg)

if __name__=="__main__":
	test()

