import sys

def value(line,name,endStr='"',start=0):
	start=line.find(name,start)
	if start < 0:
		print name
		print "start"
		return None
	start+=len(name)
	end=line.find(endStr,start)
	if end > 0:
		return line[start:end]
	return None

def strValue(line,name,start=0):
	return value(line,'"'+name+'":"',endStr='"',start=start)

def intValue(line,name,start=0):
	return value(line,'"'+name+'":',endStr=',',start=start)

def testValue():
	for line in sys.stdin:
		print line.strip()
		print strValue(line,"path")
		print strValue(line,"referer")
		print ""

if __name__=="__main__":
	testValue()

