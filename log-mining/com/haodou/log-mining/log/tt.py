import sys
sys.path.append("./")
sys.path.append("../")

from column import *

for line in sys.stdin:
	if line.find(sys.argv[1]) >= 0:
		print line.strip()


