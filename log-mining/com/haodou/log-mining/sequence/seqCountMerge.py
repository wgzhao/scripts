import sys
import math

#xx,yy,xy,x,y,n
mr={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	if len(cols) < 9:
		continue
	m=cols[0]
	if m not in mr:
		mr[m]=[0,0,0,0,0,0,0,0]
	mr[m][0]+=float(cols[1])
	mr[m][1]+=int(cols[2])
	mr[m][2]+=float(cols[3])
	mr[m][3]+=float(cols[4])
	mr[m][4]+=int(cols[5])
	mr[m][5]+=int(cols[6])
	mr[m][6]+=int(cols[7])
	mr[m][7]+=int(cols[8])

for m in mr:
	xx=mr[m][0]
	yy=mr[m][1]
	xy=mr[m][2]
	x=mr[m][3]
	y=mr[m][4]
	n=mr[m][5]
	rx=mr[m][6]
	rxy=mr[m][7]
	x1=float(x)/(n+1e-32)
	y1=float(y)/(n+1e-32)
	print "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.4f\t%.4f\t%.4f"%(m,xx,yy,xy,x,y,n,rx,rxy,rxy/(rx+1e-12),xy/(xx+1e-12),(xy-n*x1*y1)/(math.pow(xx+1e-12-n*x1*x1,0.5)*math.pow(yy+1e-12-n*y1*y1,0.5)))
	#print "%s\t%f\t%d\t%f\t%f\t%d\t%d\t%f\t%f"%(m,xx,yy,xy,x,y,n,xy/(xx+1e-12),(xy-n*x1*y1)/(math.pow(xx+1e-12-n*x1*x1,0.5)*math.pow(yy+1e-12-n*y1*y1,0.5)))


