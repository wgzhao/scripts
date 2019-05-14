
def cut(rs,N):
	return sorted(rs.items(), key=lambda d: d[1],reverse=True)[0:N]

import random
def test():
	N=5
	rs={}
	for i in range(N*2):
		rs[i]=int(3*random.random())
	print rs
	cut(rs,N)
	print rs

if __name__=="__main__":
	test()


def halfCut(ts,min=0,max=100):
	n=len(ts)/2
	if n < min:
		n=min
	if n > max:
		n=max
	if n <= 0:
		return {}
	#print n
	ret = cut(ts,n)
	#print ret
	return ret


