import time

def date(t):
	return t.tm_year*10000+t.tm_mon*100+t.tm_mday

def dateInt(tstr):
	try:
		t=time.localtime(float(tstr))
		return date(t)
	except:
		return -1

#2009-05-31 16:26:13
def dayDiv(dstr,y,m,d):
	ts=dstr.split(" ")[0].split("-")
	div=365*(y-int(ts[0]))
	div+=30*(m-int(ts[1]))
	div+=(d-int(ts[2]))
	return int(div)

def getY():
	return time.localtime().tm_year

def getM():
	return time.localtime().tm_mon

def getD():
	return time.localtime().tm_mday

def test():
	print dayDiv("2009-05-31 16:26:13",2014,5,31)

if __name__=="__main__":
	test()

