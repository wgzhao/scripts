import sys

for line in sys.stdin:
	print "\t".join(line.strip().split())


