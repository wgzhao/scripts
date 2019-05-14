#encoding=utf-8

import sys

date="_"+"".join(sys.argv[1].split("-"))
for line in sys.stdin:
	print line.strip()+date

