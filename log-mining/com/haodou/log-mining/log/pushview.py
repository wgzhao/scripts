import sys

sys.path.append("./")
sys.path.append("../abtest/")

import column2

ms={}
for line in open("message.txt"):
	ms[line.strip()]=1

def parse():
	for line in sys.stdin:
		(vn,endPos)=column2.getValue(line,'"vn":"','"',0)
		(uuid,endPos)=column2.getValue(line,'"uuid":"','"',endPos)
		(channel,endPos)=column2.getValue(line,'"channel":"','"',endPos)
		(method,endPos)=column2.getValue(line,'"method":"','"',endPos)
		(message,endPos)=column2.getValue(line,'"message":"','"',endPos)
		(server_time,endPos)=column2.getValue(line,'"server_time":','}',endPos)
		if uuid != None and method != None and message!=None and server_time != None and vn != None and channel != None:
			try:
				message=column2.escapeUnicode(message).encode("utf-8")
			except Exception:
				sys.stderr.write(line)
				continue
			has=False
			for m in ms:
				if message.find(m) > 0:
					message=m
					has=True
					break
			if has:
				continue
			print uuid+"\t"+method+"\t"+message+"\t"+server_time+"\t"+vn+"\t"+channel

if __name__=="__main__":
	parse()


