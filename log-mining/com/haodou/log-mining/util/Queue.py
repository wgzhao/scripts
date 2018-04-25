#encoding=utf-8

import sys

#只可以增加元素，不可以删除元素，当然可以覆盖元素
class AddQueue(object):
	def __init__(self,maxSize=3):
		self.list=[]
		self.maxSize=maxSize
		self.cur=-1

	def put(self,e):
		self.cur=((self.cur+1)%self.maxSize)
		if len(self.list) < self.maxSize:
			self.list.append(e)
		else:
			self.list[self.cur]=e

	def get(self):
		if self.cur >= 0:
			return self.list[self.cur]

	def __str__(self):
		s="%d\t["%(self.cur)
		for i in range(len(self.list)):
			if i == 0:
				s+=str(self.list[i])
			else:
				s+=","+str(self.list[i])
		s+="]"
		return s

if __name__ == "__main__":
	q=AddQueue(2)
	q.put(1)
	q.put(1)
	print q,q.get()
	q.put(2)
	print q
	print q.get()


