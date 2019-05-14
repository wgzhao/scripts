#encoding=utf-8

import sys

sys.path.append("./")

def escapeUnicode(v):
	if v.find("\u") >= 0:
		v=eval('u'+'"'+v+'"')
	return v


def utf8ToUnicodeStr(v):
	print v
	v=v.encode("utf-8")
	print v

if __name__=="__main__":
	utf8ToUnicodeStr(u"ABTESTING_HOME_LAYOUT(达人菜谱):B")

