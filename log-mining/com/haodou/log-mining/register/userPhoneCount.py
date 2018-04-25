#encoding=utf-8
import sys

ts={}
for line in sys.stdin:
	cols=line.strip().split()
	u=cols[0]
	phone=cols[1]
	if len(cols[1]) == 11 and cols[1].startswith("1"):
		phone=phone[0:3]
		if phone not in ts:
			ts[phone]=1
		else:
			ts[phone]+=1

for t in ts:
	print t+"\t"+str(ts[t])

