#encoding=utf-8

import random
import sys


def test():
	d={}
	for line in open("tt.1026"):
		w=line.strip()
		if len(w) ==0:continue
		if w not in d:
			d[w]=1
		else:
			d[w]+=1
	for w in d:
		if d[w] >= 2:
			print w,d[w]

def testDataForAcc():
	time=1446078785
	for i in range(200):
		t=time+random.randrange(1000)*86400+random.randrange(1000)
		if i < 1:
			print "2345\tui\trid-234629\tview\t"+str(t)
		print "rid-123\tiu\t2345\tview\t"+str(t)
		t=time+random.randrange(1000)*86400+random.randrange(1000)
		print "rid-123\tiu\t2345\ttag\t"+str(t)
		t=time+random.randrange(1000)*86400+random.randrange(1000)
		print "rid-123\tiu\t2345\ttag\t"+str(t)

def appendType():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		print cols[0]+"\tui\t"+"\t".join(cols[1:])

if __name__=="__main__":
	#test()
	#testDataForAcc()
	appendType()

