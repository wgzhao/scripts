import sys

sys.path.append("./")
sys.path.append("../")

from column import *
from dictInfo import *

MinSessionDiv=300
SmoothTimeDiv=1

def count(f):
	for line in f:
		if line.find("ad.getxfad") > 0 or line.find("ad.getallad") > 0 or line.find("mobiledevice.initandroiddevice") > 0 or line.find("ad.getadinmobi") > 0 or line.find("ad.getad_imocha") > 0 or line.find("ad.getad_imochav2") > 0:
			sys.stderr.write(line)
			continue
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		try:
			time=int(cols[0])
		except Exception:
			sys.stderr.write(line)
			continue
		uid=uuidFirst(cols)
		if uid == None or uid == "":
			continue
		print uid+"\t"+cols[0]

if __name__=="__main__":
	count(sys.stdin)

