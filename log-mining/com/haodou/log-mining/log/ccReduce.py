import sys

sys.path.append("./")

import ccinfo

def count(f):
	cc={}
	for line in f:
		c=int(line.strip().split("\t")[0])
		if c not in cc:
			cc[c]=1
		else:
			cc[c]+=1
	for c in cc:
		print "%d\t%d"%(c,cc[c])
	print ccinfo.info(cc)

if __name__=="__main__":
	count(sys.stdin)


