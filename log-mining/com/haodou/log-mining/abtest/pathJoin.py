import sys
import time
import datetime

def path(dir,begin,end):
	bt=time.strptime(begin,"%Y-%m-%d")
	et=time.strptime(end,"%Y-%m-%d")
	bit = int(time.mktime(bt))
	eit = int(time.mktime(et))
	s=""
	while bit <= eit:
		begin = time.strftime("%Y-%m-%d",bt)
		if len(s) > 0:
			s+=","
		s+=dir+"/"+begin
		bdt = datetime.datetime.fromtimestamp(bit) 
		bdt += datetime.timedelta(days = 1)
		bit = int(time.mktime(bdt.timetuple()))
		bt = time.localtime(bit)
	return s

def response(begin,end):
	print path("/user/yarn/logs/source-log.php.CDA39907.resp",begin,end)

def request(begin,end):
	print path("/user/yarn/logs/source-log.php.CDA39907",begin,end)


if __name__=="__main__":
	if len(sys.argv) >= 4:
		if sys.argv[1] == "request":
			request(sys.argv[2],sys.argv[3])
		elif sys.argv[1] == "response":
			response(sys.argv[2],sys.argv[3])


