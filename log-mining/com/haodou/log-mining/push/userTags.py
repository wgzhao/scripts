import sys

for line in sys.stdin:
	cols=line.strip().split("\t")
	if cols[1] == "":
		continue
	print cols[0]+"\t"+cols[1]


