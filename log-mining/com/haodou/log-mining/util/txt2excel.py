#encoding=utf-8

from XLSWriter import *
import sys
#import codecs

def checkType(c):
	try:
		c=int(c)
		return c
	except:
		pass
	try:
		c=float(c)
		return c
	except:
		pass
	return c


def getName(path):
	p=path.rfind("/")
	return path[p+1:]

def txt2excel(txtFiles,excelFile,sheetNames=None):
	if excelFile == None:
		excelFile=txtFiles[0]+".xls"
	xlswriter = XLSWriter(excelFile)
	for i in range(len(txtFiles)):
		txtFile=txtFiles[i]
		if len(txtFile) == 0:
			continue
		if sheetNames == None:
			sheetName=getName(txtFile)
		else:
			sheetName=sheetNames[i]
		sheetName=sheetName.decode("utf-8")
		for line in open(txtFile):
			cols=line.strip().split("\t")
			xlswriter.writerow([checkType(c) for c in cols], sheetName)
	xlswriter.save()

if __name__=="__main__":
	txtFiles=sys.argv[1]
	excelFile=None
	if len(sys.argv) >= 3:
		excelFile=sys.argv[2]
	sheetName=None
	if len(sys.argv) >= 4:
		sheetName=sys.argv[3]
	txt2excel(txtFiles.split(","),excelFile,sheetName.split(","))


