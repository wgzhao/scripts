#encoding=utf-8

import sys

n=0
for line in sys.stdin:
	n+=1
	cols=line.strip().split("\t")
	if n == 1:
		cols[0]="策略"
	elif n == 2:
		cols[0]="A"
		sum=int(sys.argv[1])
	else:
		cols[0]="B"
		sum=int(sys.argv[2])
	for i in range(1,len(cols),2):
		if n > 1:
			cols[i+1]=str(float(cols[i])/sum)
		else:
			cols[i+1]="均值"
	print "\t".join(cols)

	
