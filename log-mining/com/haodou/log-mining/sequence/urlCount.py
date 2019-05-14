#encoding=utf-8

import sys

def count(url):
	ips={}
	pv=0
	nips={}
	npv=0
	for line in sys.stdin:
		cols=line.strip().split('\01')
		if len(cols) < 6:
			continue
		if cols[4] == url:
			pv+=1
			ip=cols[0]
			if ip not in ips:
				ips[ip]=1
			else:
				ips[ip]+=1
		if line.strip().find(url) > 0:
			npv+=1
			ip=cols[0]
			if ip not in nips:
				nips[ip]=1
			else:
				nips[ip]+=1
	#print "pv:",pv
	#print "uv:",len(ips)
	#print "npv:",npv
	#print "nuv:",len(nips)
	print "%d\t%d"%(pv,len(nips))

if __name__=="__main__":
	count("/app/recipe/act/novice.php")

