#encoding=utf-8

import sys

from conf import *


class Bank(object):
	def __init__(self):
		self.bank=0
		self.price=0
		
	#返回收益变化
	def addBank(self,bank,price,conf):
		if abs(bank) <= 0:
			return (0,0)
		e=0
		flow=0
		if self.bank == 0:
			self.price=price
			flow-=abs(bank)*price/conf.GangRate #create
		elif self.bank > 0:
			if bank > 0:
				self.price=(self.bank*self.price+bank*price)/(self.bank+bank)
				flow-=bank*price/conf.GangRate #create
			else:
				if self.bank + bank > 0:
					e=-bank*(price-self.price)
					flow+=abs(bank)*self.price/conf.GangRate #destroy
					flow+=abs(bank)*(price-self.price) #e
				else:
					e=self.bank*(price-self.price)
					flow+=abs(self.bank)*self.price/conf.GangRate #destroy
					flow+=(self.bank+bank)*price/conf.GangRate #create
					flow+=self.bank*(price-self.price) #e
					self.price=price
					#print "inner,e",e,"flow",flow
		else:
			if bank < 0: #self.bank < 0
				self.price=(self.bank*self.price+bank*price)/(self.bank+bank)
				flow-=abs(bank)*price/conf.GangRate #create
			else:
				if self.bank+bank < 0:
					e=-bank*(price-self.price)
					flow+=abs(bank)*self.price/conf.GangRate #destroy
					flow+=(-bank)*(price-self.price) #e
				else:
					e=self.bank*(price-self.price)
					flow+=abs(self.bank)*self.price/conf.GangRate #destroy
					flow-=(bank+self.bank)*price/conf.GangRate #create
					flow+=self.bank*(price-self.price) #e
					self.price=price
		self.bank+=bank
		e-=abs(bank)*price*conf.Rate
		flow-=abs(bank)*price*conf.Rate #e
		#print "e",e,"flow",flow
		#print bank,self.bank
		return (e*conf.HandSize,flow*conf.HandSize)

	def destroy(self,price,conf):
		return self.addBank(-self.bank,price,conf)
		
	def value(self,conf=basicConf):
		return abs(self.bank*self.price)*conf.HandSize

	def __str__(self):
		s="%.4f\t%.4f"%(self.bank,self.price)
		return s


def test():
	b=Bank()
	print b.addBank(1,1.2)
	print b
	print b.addBank(0.1,0.5)
	print b
	print b.addBank(-1.0,2.0)
	print b
	print b.addBank(-1.0,0.1)
	print b
	print b.addBank(0,0.4)
	print b
	print b.addBank(-0.4,4.0)
	print b

if __name__=="__main__":
	test()


