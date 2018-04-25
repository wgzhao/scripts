#encoding=utf-8

import math
import random
import copy
import os

#SVMSize=60
#SVMMidSize=30
#SVMTSize=5

class Conf(object):
	def __init__(self):
		self.CurveFix="__Curve__"
		self.Size=100
		#self.PSize=self.Size/4 #Size/4
		self.MidSize=120 #Size*11/10
		self.TT=self.MidSize
		self.TSize=self.MidSize/self.TT
		self.TPos=0
		self.StopLossNearInt=0.51
		self.DayEnd=1425
		self.DayStart=935
		self.DivSize=50 #self.MidSize
		self.GangRate=20.0/3
		self.Rate=0.00006
		self.UnitLimit=0.025
		self.unitLowRate=1.0
		self.BankRate=0.95
		self.a1Rate=1.0/30000
		self.a1Discount=1.0
		self.HandSize=300
		self.StartValue=3000000.0
		self.SlideRate=0.2
		self.MaxLoss=60.0*(1.0/30000+1.0/30000)
		self.LSize=20
		self.MaxEarn=10*self.MaxLoss #600.0*(1.0/30000+1.0/30000)
		self.AdjustRate=0.1*self.MaxLoss
		self.Stock=False
		self.SmallDiv=3
		self.LargeDiv=1.5
		#self.MDiv=10 #检测宏观环境
		#self.divRate=0.9
		self.a1DivRate=0.0
		self.MoveLossRate=3.0/self.MidSize
		self.Delay=0
		#self.DataType="ZJIF00"
		self.DataType="dce"
		#self.DataType="zj0616"
		self.YearFold=270*250.0
		self.DType="strong"
		self.TickDiv=0.8*self.MaxLoss
		self.LongDiv=-0.1*self.MaxLoss
		self.Only=False
		if self.TSize == self.MidSize:
			self.Only=True

	def getSizes(self):
		return [
			self.MidSize,
			self.Size,
			#int(self.Size*self.LargeDiv*self.MDiv),
			int(self.Size*self.LargeDiv),
			int(self.Size/self.SmallDiv),
		]

class SlideConf(Conf):
	def __init__(self):
		Conf.__init__(self)

class PricePhaseConf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.MidSize=140
		self.SmallSize=20
		self.Size=40
		self.LargeSize=80
		self.MSize=150
		self.Div=3

class DCEConf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.DataType="dce"
		self.MaxLoss*=2
		self.MaxEarn*=2
		self.Size=150
		self.LargeSize=600
		self.SmallSize=30
		self.MidSize=150
		self.HandSize=10.0
		self.MaxLoss=0.003
		self.MaxEarn=10*self.MaxLoss
		self.AdjustRate=1.0*self.MaxLoss


class TickConf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.Rate=0.00005
		self.SlideRate=1.0
		self.a1Rate=0.5/30000
		self.a1Discount=1.0
		self.SmallDiv=3 #10
		self.LargeDiv=1.5 #22
		self.Size=12000 #400
		self.MidSize=14400 #480
		self.TSize=self.MidSize/12
		#self.AdjustRate=1.0*self.MaxLoss
		#self.MoveLossRate=2.0/self.MidSize
		self.MaxLoss=100.0*(1.0/30000+1.0/30000) #50.0*(1.0/30000+1.0/30000)
		self.MaxEarn=1000.0*(1.0/30000+1.0/30000) #120.0*(1.0/30000+1.0/30000)
		self.TickDiv=0.15*self.MaxLoss
		self.LongDiv=-0.3*self.MaxLoss
		self.DataType="tick"
		self.DType="strong"

class V1Conf(Conf):
	def __init__(self,v1c=1):
		Conf.__init__(self)
		self.DataType="v1"
		self.V1DataColumn=v1c

class MultiConf(Conf):
	def __init__(self,v1c=1):
		Conf.__init__(self)
		self.DataType="multi"
		self.V1DataColumn=v1c

class Arbitrage(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.DataType="arbitrage"
		self.Size=120
		self.MaxLoss=0.1
		self.MaxEarn=1.0

class NoA1Conf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.a1Rate*=0.0

class ChangeLoss(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.MaxLoss*=0.5

def abs(x):
	if x < 0:
		return -x
	return x

def near(f):
	fn=math.floor(f)
	fn1=math.ceil(f)
	if abs(f-fn) <= abs(fn1-f):
		return (fn,fn-f)
	return (fn1,fn1-f)

class DayConf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.Size=100
		self.MidSize=120
		self.Rate=0.0005
		self.a1Rate=1.0/30000
		self.MaxLoss=50*(self.a1Rate+1.0/30000)
		self.MaxEarn=100*(self.a1Rate+1.0/30000)
		self.Stock=False
		self.HandSize=1.0
		self.GangRate=1.0
		self.TSize=self.MidSize/12
		self.BankRate=0.9
		self.a1DivRate=0.0
		self.MoveLossRate=0.0*self.TSize/self.MidSize
		self.StartValue=1000000.0
		self.Delay=0
		self.DataType="stock"
		
class StockDayConf(Conf):
	def __init__(self):
		Conf.__init__(self)
		self.StartTime="2006-01"
		self.EndTime="2014-12"
		self.Size=100
		self.MidSize=120
		self.SmallDiv=3
		self.LargeDiv=1.5
		#self.LargeSize=20
		self.Rate=0.001
		self.a1Rate=1.0/3000
		self.MaxLoss=20.0/30
		self.MaxEarn=20.0/3
		self.Stock=True
		self.HandSize=100
		self.GangRate=1.0
		self.TSize=self.MidSize/12
		self.BankRate=0.5
		self.MoveLossRate=2.0/self.MidSize
		self.StartValue=100000.0
		self.Delay=0
		self.DataType="stockDay"
		self.SlideRate=0.0
		self.YearFold=250.0

class StockWeekConf(StockDayConf):
	def __init__(self):
		StockDayConf.__init__(self)
		self.YearFold=50.0

class DaySizeConf(DayConf):
	def __init__(self,MidSize=720):
		DayConf.__init__(self)
		self.MidSize=MidSize
		self.Size=self.MidSize*2/3
		self.a1Rate=1.0/30000*math.pow(120.0/MidSize,0.5)
		self.MaxLoss=50.0*math.pow(MidSize/120.0,0.5)*(self.a1Rate+1.0/30000)
		self.MaxEarn=10.0*self.MaxLoss
		self.TSize=self.MidSize/12

basicConf=Conf()
dayConf=DayConf()

def getFiles(d):
	ret=[]
	files=os.listdir(d)
	for file in files:
		if os.path.isdir(d+"/"+file):
			ret.extend(getFiles(d+"/"+file))
		else:
			ret.append(d+"/"+file)
	return sorted(ret)

if __name__=="__main__":
	print near(4.6)
	print near(-2.3)
	print near(-2.8)
	print dayConf.Size
	print getFiles("/home/zhangzhonghui/libsvm/data")

