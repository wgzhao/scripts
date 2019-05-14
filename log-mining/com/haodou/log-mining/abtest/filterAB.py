import sys

sys.path.append("./")
sys.path.append("../")

from column import *

def readUser(file,name):
	us={}
	for line in open(file):
		cols=line.strip().split("\t")
		if cols[0] == name:
			us[cols[1]]=1
	return us

def filter(f,us,timeMin,timeMax):
	for line in f:
		cols=line.strip().split("\t")
		u=uuidFirst(cols)
		if u == None or len(u)== 0:
			continue
		try:
			time=int(cols[0])
		except Exception:
			continue
		if time < timeMin:
			continue
		if timeMax > 0 and time > timeMax:
			continue
		if u in us:
			print line.strip()

if __name__=="__main__":
	us=readUser(sys.argv[1],sys.argv[2])
	sys.stderr.write("number of user for "+sys.argv[2]+" is "+str(len(us))+"\n")
	timeMin=0
	if len(sys.argv) >= 4:
		timeMin=int(sys.argv[3])
	timeMax=-1
	if len(sys.argv) >= 5:
		timeMax=int(sys.argv[4])
	sys.stderr.write("timeMin:"+str(timeMin)+"\n")
	sys.stderr.write("timeMax:"+str(timeMax)+"\n")
	filter(sys.stdin,us,timeMin,timeMax)



