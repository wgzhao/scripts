import sys

for line in sys.stdin:
	cols=line.strip().split("\t")
	if len(cols) < 3:
		continue
	print cols[1]+"\t"+cols[0]+"\t"+cols[2]+"\tu"



