import sys

rc={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	rid=cols[1]
	
