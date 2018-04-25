import sys
import math

def entropy(cc):
	sum=0.0
	for c in cc:
		sum+=c*cc[c]
	if sum <= 0:
		return 0
	ent=0
	for c in cc:
		if c > 0:
			ent-=cc[c]*math.log(c/sum)*c/sum
	return ent

def phase(c):
	s=""
	if c < 5:
		s=str(c)
	elif c >= 5 and c <= 15:
		s=str("5-15")
	elif c >= 16 and c <= 99:
		s="16-99"
	elif c >= 100 and c <= 199:
		s="100-199"
	elif c >= 200 and c <= 999:
		s="200-999"
	elif c >= 1000 and c <= 1999:
		s="1000-1999"
	elif c >= 2000 and c <= 4999:
		s="2000-4999"
	elif c >= 5000 and c <= 9999:
		s="5000-9999"
	elif c >= 10000 and c <= 99999:
		s="10000-99999"
	else:
		s=">=100000"
	return "range("+s+")"

def info(cc):
	if len(cc) == 0:
		return None
	s={}
	sum=0
	sum2=0
	max=0
	min=1000000
	size=0
	for c in cc:
		sum+=c*cc[c]
		if c < min:
			min=c
		if c > max:
			max=c
		sc=phase(c)
		if sc not in s:
			s[sc]=cc[c]
		else:
			s[sc]+=cc[c]
		if c > 0:
			sum2+=cc[c]*math.pow(c,2.0)
		size+=cc[c]
	avg=float(sum)/size
	s["avg"]=avg
	s["sum"]=sum
	s["num"]=size
	s["max"]=max
	s["min"]=min
	asum=0
	d2=0
	for c in cc:
		if c > 10*avg:
			asum+=c*cc[c]
		d2+=math.pow(c-avg,2.0)*cc[c]
	s["stddev"]=math.pow(d2/size,0.5)
	if sum > 0:
		s["exceptRate"]=asum/float(sum)
	else:
		s["exceptRate"]=0
	s["entropy"]=entropy(cc)
	return s

def dict2cc(d):
	cc={}
	for w in d:
		c=int(d[w])
		if c not in cc:
			cc[c]=1
		else:
			cc[c]+=1
	return cc

if __name__=="__main__":
	for i in range(100,10000,100):
		print i,phase(i)

