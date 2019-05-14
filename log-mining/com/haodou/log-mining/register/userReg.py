#encoding=utf-8

def readUser(f):
	uds={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) == 2:
			(d,u)=cols
			if u not in uds:
				uds[u]=[]
			uds[u].append(d)
	return uds

def userReg(f,uds):
	ds={}
	for line in f:
		cols=line.strip().split("\t")
		(u,phone,reg)=cols
		reg=reg[0:7]
		if u in uds:
			for d in uds[u]:
				if d not in ds:
					ds[d]={}
				if reg not in ds[d]:
					ds[d][reg]=1
				else:
					ds[d][reg]+=1
	for d in ds:
		sum=1e-32
		for reg in ds[d]:
			sum+=ds[d][reg]
		for reg in ds[d]:
			print "%s\t%s\t%d\t%.4f"%(d,reg,ds[d][reg],ds[d][reg]/sum)

if __name__=="__main__":
	uds=readUser(open("/home/zhangzhonghui/data/shqu.txt"))
	userReg(open("/home/zhangzhonghui/data/userPhone.txt"),uds)

