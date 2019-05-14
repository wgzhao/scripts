import sys

Min=3
if len(sys.argv) >= 2:
	Min=int(sys.argv[1])
Rate=0.1
if len(sys.argv) >= 3:
	Rate=float(sys.argv[2])
Smooth=10
if len(sys.argv) >= 4:
	Smooth=float(sys.argv[3])


def output(lastU,ts,sum):
	for t in ts:
		if ts[t] >= Min and ts[t]/float(sum+Smooth) >= Rate:
			print lastU+"\t"+t+"\t"+str(ts[t])+"\t"+str(sum)

def count():
	lastU=""
	ts={}
	sum=0
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU=="":
			lastU=u
		if lastU != u:
			output(lastU,ts,sum)
			lastU=u
			ts={}
			sum=0
		for i in range(2,len(cols),1):
			t=cols[i]
			if t not in ts:
				ts[t]=1
			else:
				ts[t]+=1
		sum+=1
	if lastU != "":
		output(lastU,ts,sum)

if __name__=="__main__":
	count()


