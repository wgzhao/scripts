import sys

lines=[]
for line in sys.stdin:
	cols=line.strip().split()
	lines.append("\t".join(cols))

for i in range(len(lines)-1,-1,-1):
	print lines[i]



