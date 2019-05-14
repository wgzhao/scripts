import sys
import codecs

ProjectCoding="utf-8"

def ustr(s):
	if s == None:
		return s
	if type(s) == unicode:
		return en(s)
	return str(s)

def un(s):
	return s.decode(ProjectCoding)

def en(s):
	return s.encode(ProjectCoding)

def uopen(file):
	return codecs.open(file,"r",ProjectCoding)


