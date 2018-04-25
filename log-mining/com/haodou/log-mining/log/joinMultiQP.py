# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 14:59:56 2014

@author: Yanghong
"""

#连接请求日志和响应日志,并对多条请求和多条应答对应同一个requestid的情况按时间进行处理


import sys
import time

sys.path.append("./")
sys.path.append("../")
sys.path.append("../mlog")
sys.path.append("../abtest")

import column
import mlog
import random
import datetime

def toIntTime(s):
    #datetime.datetime.i
    return int(time.mktime(time.strptime(s,'%d/%b/%Y:%H:%M:%S')))
    

#处理request\response\m_haodou_com日志
#输出requestId + '\t' + tiem + '\t' + line
def qid_m(f):
    for line in f:
        line=line.strip()
        if line.startswith('{"status":'): #response日志
            q=column.getQid2(line,"request_id")
            time=line.strip().split('"time":')[-1][:-1] #获取response的时间
            if q != None and q != "":
                print q+"\t"+time+'\t'+line
        elif line[0:4].find(".") > 0: #m_haodou_com日志
            cols=line.strip().split("\01")
            if len(cols) > 5:
                ip=cols[0]
                try:
                    time=str(toIntTime(cols[2].split()[0]))
                    print str(random.randint(0,99))+"\t"+ip+"\t"+time+"\t"+line.strip()
                except:
                    sys.stderr.write(line)
                    continue
        else: #请求日志
            cols=line.split("\t")
            if len(cols) < column.APP_LOG_COLUMNS:
                sys.stderr.write(line)
                continue
            q=column.getValue(cols[column.PARA_ID],"request_id")
            if q != None and q != "":
                print q+"\t"+line
            else:
                print str(random.randint(0,99))+"\t"+line



def output(method,qs,ps):
    qs_len=len(qs)
    ps_len=len(ps)
    if qs_len>1 :
       qs=sorted(qs)  #按时间排序
    if ps_len>1:        
       ps=sorted(ps)  #按时间排序
    #if qs_len==ps_len: #时间一一对应,且记录条对应
    offset = 0
    if qs_len ==0 :
        return
    if ps_len == 0: #只有request没有对应的response
        for q in qs:
            if len(q)>=2:
                print q[1]
    else:
        for i in range(qs_len):
            if True:
                j=0 #当对应记录时间顺序上不匹配时，计算时间偏移
                if offset < ps_len and (qs[i][0] == ps[offset][0]) : #时间匹配
                    print qs[i][1]+"\t"+ps[offset][1]
                elif offset < ps_len and qs[i][0] < ps[offset][0] :  
                    if (i+1 >= qs_len) or  qs[i+1][0] > ps[offset][0] :  #response的时间大于当前请求的时间，但小于下一请求的时间，一般发生在请求和返回的时间差
                        print qs[i][1]+"\t"+ps[offset][1]
                    else:
                        j += 1
                        offset = i- j 
                        print qs[i][1]
                elif offset <  ps_len and qs[i][0] > ps[offset][0]:
                    for k in range(i,ps_len):
                        #j=k
                        offset = k
                        #errLog.write('ps_len:' + str(ps_len) + ' offset='+str(offset)+'\n')
                        if qs[i][0] <= ps[offset][0]: break
                    if offset < ps_len:
                        print qs[i][1]+"\t"+ps[offset][1]
                    else:
                        print qs[i][1]
                else:
                    print qs[i][1]
                    #j += 1
            #except Exception:
            #    sys.stderr.write(
			#	continue
    
def qidReduce(f):
    lastId=""
    method=""
    qs=[]
    qt=[]
    ps=[]
    pt=[]
    for line in f:
        cols=line.strip().split("\t")
        if len(cols) < 3:
            continue
        id=cols[0]  #获得request_id,对于m_haodou_com日志，id为<100的随机数，对于request_id为空的请求，id也为<100的随机数
        if lastId == "":
            lastId=id
        if lastId != id: #下一个请求
            output(method,qs,ps) #将前面的输出
            lastId=id
            qs=[]
            qt=[]
            ps=[]
            pt=[]
            method=""
        if len(id) <= 2 or len(cols) > 3: #处理request和m_haodou_com的记录
            #uid=column.ActionUser(cols[1:])
            if len(cols) > 4: #request是以'\t'分隔，列数>4
                cols[0]=cols[column.IP_CID+1] #将ip地址放到行首，后面接原来的request
                time=cols[1] #获得时间
                q="\t".join(cols)
                method=cols[column.METHOD_CID+1] #获取请求的调用方法
            else: #m_haodou_com的记录
                time = cols[2]
                q="\t".join(cols[1:]) #
            qt.append(time)
            qs.append((time,q)) #将请求加入列表
            #sys.stderr.write(q+'\n')
        else: #处理response
            p=cols[2]
            time=cols[1]
            pt.append(time)
            ps.append((time,p))
    if lastId != "":
        output(method,qs,ps)

if __name__=="__main__":
    #print toIntTime("01/Dec/2014:00:00:00")
    starttime=time.clock()
    if len(sys.argv) >= 2:
        if sys.argv[1] == "qid":
            qid_m(sys.stdin)
        elif sys.argv[1] == "qidReduce":
            qidReduce(sys.stdin)
    else:
        qidReduce(sys.stdin)
    endtime=time.clock()
    sys.stderr.write("%.3f\n"%(endtime - starttime))
