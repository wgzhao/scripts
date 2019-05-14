#encoding=utf-8

import sys
from conf import *
import math

f=open(sys.argv[1],"w")

div=100
if len(sys.argv) >= 3:
	div=int(sys.argv[2])

nvg=0
t=0
for line in sys.stdin:
	if line.startswith(basicConf.CurveFix):
		v=float(line[len(basicConf.CurveFix):-1])
		t+=1
		nvg+=v
		if t % div == 0:
			f.write("%.4f\n"%(math.log(nvg/div)))
			#f.write("%.4f\n"%(nvg/div))
			nvg=0
	else:
		print line.strip()
if t % div != 0:
	f.write("%.4f\n"%(math.log(nvg/(t%div))))
	#f.write("%.4f\n"%(nvg/(t%div)))
f.close()


