#encoding=utf-8

from conf import *
import wave
import data

def duralTrust(f,conf=basicConf,arg=None):
	bank=bankManage.Bank()
	detail=(arg=="detail")
	start=conf.StartValue
	curve=loss.Loss(start)
	tt=0
	phase=wave.Phase(conf.getSizes())
	blist=Stack.Queue(conf.MidSize/conf.TSize+1,[0,0,0,0])
	restBank=[0,0] #余量窗口
	minFlow=curve.now
	flow=curve.now
	for line in f:
		tt+=1
		lastD,nowD=data.readLine(line,conf)
		if nowD == None:
			continue
		v0=nowD.lastP
		v1=nowD.p
		phase.addV(v0)
		

