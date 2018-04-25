class Queue(object):
	def __init__(self,N):
		self.N=N
		self.list=[]
		self.pos=0
		self.size=0
	
	def contains(self,e):
		if e in self.list:
			return True
		return False

	def add(self,e):
		self.size+=1
		if self.contains(e):
			return
		if len(self.list) < self.N:
			self.list.append(e)
		else:
			self.list[self.pos]=e
		self.pos=(self.pos+1)%self.N

def testQueue():
	q=Queue(5)
	import random
	for i in range(100):
		n=random.randint(0,10)
		q.add(n)
		print n,q.list,q.pos,q.size

if __name__=="__main__":
	testQueue()

