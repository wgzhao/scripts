import sys

from pathJoin import *

import readConf
if __name__=="__main__":
	if len(sys.argv) >= 2:
		d=readConf.readConf()
		if sys.argv[1] == "request":
			request(d["begin"],d["end"])
		elif sys.argv[1] == "response":
			response(d["begin"],d["end"])
		elif sys.argv[1] == "request_user":
			request(d["begin_user"],d["end_user"])
		elif sys.argv[1] == "response_user":
			response(d["begin_user"],d["end_user"])

