import sys
import csv
import codecs

def txt2csv(txtFile,charset="gbk",csvFile=None):
	if csvFile == None:
		csvFile=txtFile+"."+charset+".csv"
	fileobj=open(csvFile,"wb")
	fileobj.write('\xEF\xBB\xBF')
	writer=csv.writer(fileobj,dialect='excel')
	for line in codecs.open(txtFile,"r","utf-8"):
		cols=line.encode(charset).strip().split("\t")
		writer.writerow(cols)
	fileobj.close()

if __name__=="__main__":
	if len(sys.argv) >= 3:
		txt2csv(sys.argv[1],charset=sys.argv[2])
	else:
		txt2csv(sys.argv[1])


