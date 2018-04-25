#encoding=utf-8

import sys
sys.path.append("./")

def uuid():
	for line in sys.stdin:
		if line.startswith("deviceid:") or line.startswith("uid:") or line.startswith("haodou"):
			#sys.stderr.write(line)
			continue
		if line.startswith("uuid:"):
			line=line[len("uuid:"):]
		print line.strip()


if __name__ == "__main__":
	uuid()

