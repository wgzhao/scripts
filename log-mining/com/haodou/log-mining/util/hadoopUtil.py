#encoding=utf-8
import sys

def split(s):
	return " ".join(s.split(","))

if __name__=="__main__":
	if sys.argv[1] == "split":
		print split(sys.argv[2]),

