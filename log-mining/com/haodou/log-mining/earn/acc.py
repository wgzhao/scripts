#encoding=utf-8

import sys
from conf import *

def readTags(file):
	tags=[]
	for line in open(file):
		t=line.strip().split()[0]
		tags.append(t)
	return tags

orig=readTags(sys.argv[1])
predict=readTags(sys.argv[2])

right=0
total=0
d={}
for i in range(len(orig)):
	if random.random() > SampleRate:
		continue
	o=orig[i]
	t=predict[i]
	if o not in d:
		d[o]=[0,0,0]
	d[o][0]+=1
	if t not in d:
		d[t]=[0,0,0]
	d[t][1]+=1
	if o == t:
		d[o][2]+=1
		right+=1
	total+=1

for t in d:
	(n,tn,rn)=d[t]
	acc=0
	if tn > 0:
		acc=float(rn)/tn
	recall=0
	if n > 0:
		recall=float(rn)/n
	f1=0
	if acc+recall > 0:
		f1=2*acc*recall/(acc+recall)
	print "%s\t%d\t%d\t%d\t%.4f\t%.4f\t%.4f"%(t,n,tn,rn,acc,recall,f1)
print "right:%d\ttotal:%d\tacc:%f"%(right,total,float(right)/total)

