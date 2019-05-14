#encoding=utf-8

class Doc(object):
	def __init__(self):
		pass

class Para(object):
	def __init__(self):
		pass

class Sen(object):
	def __init__(self):
		pass

class Phrase(object):
	def __init__(self):
		pass

class Char(object):
	def __init__(self):
		pass

	def str(self):
		if not hasattr(self,"name"):
			self.name=""
			print "haha"
		print "name",self.name
		return self.name

print dir(Char().str)
print Char().str.__func__(Char())
print dir(11)
print __doc__
import data
print data.__doc__
print data.__file__

def add():
	return 2+3

print add.func_code.co_names


