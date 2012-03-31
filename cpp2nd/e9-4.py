#/usr/bin/env python
from sys import argv
from os import system
def byLineReader(filename):
    f = open(filename)
    line = f.readline()
    while line:
        yield line
        line = f.readline()
    f.close()
    yield None

def ex9_4(filename):
    '''
    a filter for paging through text one screenful at a time
    similar to more(1)
    '''
    reader = byLineReader(filename)
    line = reader.next()
    count = 25
    index = 1
    while line:
		if index > count:
			system('read -n 1 -p "Press any key to continue"') #linux
			index = 1
		index = index + 1
		print line,
		line = reader.next()
      
        
if __name__ == "__main__":
    ex9_4("/etc/hosts")
