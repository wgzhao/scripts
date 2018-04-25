#encoding=utf-8

def textEffect(line,s,e,effectStart,effectEnd):
	if s < 0 or e < 0 or s >= len(line) or e >= len(line) or s >= e:
		return line
	return line[0:s]+effectStart+line[s:e]+effectEnd+line[e:]

def bold(line,s,e):
	return textEffect(line,s,e,"<b>","</b>")


def boldSub(line,w):
	if w == None or len(w) == "":
		return line
	s=line.find(w)
	if s < 0:
		return line
	e=s+len(w)
	return bold(line,s,e)


def boldSub2(line,w1,w2):
	return boldSub(boldSub(line,w1),w2)

def split(line,w1,w2):
	s1=line.find(w1)
	s2=line.find(w2)
	slist=sorted([0,s1,s2,s1+len(w1),s2+len(w2),len(line)])
	rs=[]
	for i in range(5):
		rs.append(line[slist[i]:slist[i+1]])
	return rs

def test():
	#print boldSub2("1234yurnnkk90","4y","nkk")
	print split("1234567890","345","")	

if __name__=="__main__":
	test()



