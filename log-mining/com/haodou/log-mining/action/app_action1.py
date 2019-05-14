# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 10:59:44 2014

@author: Administrator
"""

import sys
import time

def getValString(data,key):
    len_key = len(key)
    p1 = data.find(key) + len_key
    if p1 < len_key:
        return ""
    p2 = data.find('"',p1)
    if p2==-1:
        return ""
    return data[p1:p2]
    
def getValFloat(data,key):
    len_key = len(key)
    p1 = data.find(key) + len_key
    if p1 < len_key:
        return 0.0
    p2 = data.find('}',p1)
    if p2==-1:
        p2 = data.find(',',p1)
        if p2 == -1:
            return 0.0
    try:
        t = float(data[p1:p2])
    except Exception:
        return 0.0
    return t
    

def getUserIp(data):
    key = '"user_ip":"'
    return getValString(data,key)
    
  
def getUuid(data):
    key = '"a":"'
    return getValString(data,key)


#得到app的版本，版本4.4及以后的版本号在“e"字段，以前的在”d”字段,如果是4.4版本以后，在返回版本号，4.4以前版本返回空串
def getVer44(data):
    key = '"e":"'
    ver = getValString(data,key)
    if len(ver) < 2:
        return ""
    return ver

#得到server_time，结果返回是nuix时间戳方式,为了与ver4.4以后的版本的time带有小数点兼容，返回float
def getServerTime(data):
    key = '"server_time":'
    return getValFloat(data,key)
    
def getDeviceTime(data):
    key = '"device_time":'
    return getValFloat(data,key)
    

#输入参数，app_action日志数据，版本号
#返回参数，一个包含(PAGE,ACTION,TIME)三元组的列表
def getPageAction(data,ver):
    rtn=[]
    p1 = 0
    p2=0
    while(p1 >= 0):
        #print p1,p2
        p1=data.find('"action"',p2)        
        if p1 == -1 : continue
        while (data[p1]!='{' and data[p1]!='}' and p1 >=p2):  #因为是字典，不是序列，所以json中的page可能在“action”前面，因此查找“action”前面的“{”或者“}”符号作为这个item的开始
            p1 -= 1
        p1+=1
        p2 = data.find('}',p1)
        if p2 == -1 : continue
        substr=data[p1:p2]
        p1 = p2
        
        ACTION=getAction(substr)
        if len(ACTION) == 0 :
            continue
        PAGE=getPage(substr)
        if len(PAGE)==0 :
            continue
        
        if len(ver)==0: #4.4以前版本
            TIME=getServerTime(data)
        else: #4.4以及以后版本
            TIME=getExtTime(substr)
        rtn.append((PAGE,ACTION,TIME))
    return rtn
        
        
#由getPageAction调用
def getAction(data):
    key='"action":"'
    return getValString(data,key)

def getPage(data):
    key = '"page":"'
    return getValString(data,key)


#由getPageAction调用
def getExtTime(data):
    key = '"time":"'
    tStr = getValString(data,key)
    if len(tStr)==0: return 0.0
    t1 = tStr.split('.') #将秒的小数点分离
    try:
        if len(t1)>=2:
            TIME = time.mktime(time.strptime(t1[0],'%Y-%m-%d %H:%M:%S')) + float('0.'+t1[1])
        else:
            TIME = time.mktime(time.strptime(t1[0],'%Y-%m-%d %H:%M:%S'))
    except Exception:
            return 0.0
    return TIME


#filterAction=set(['A1016'])
filterAction=[]

def action_mapper(f):
    for data in f:
        try:
            #data=json.loads(line) #数据为json格式，先进行解包
            
            #获取IP
            '''
            IP = getUserIp(data)
            if len(IP)==0:
                continue
            
            UUID= getUuid(data)
            '''

            VER = getVer44(data)
            PageActionList=getPageAction(data,VER)
            for item in PageActionList:  #将4.4.0版本的数据拆分成多条数据
                #获取页面
                PAGE=item[0]
                if len(PAGE)==0 :
                   continue

                #获取动作
                ACTION =  item[1]
                if ACTION in filterAction or len(ACTION)==0 :
                   continue


                #获取时间        
                #TIME = item[2]
                    
                #print IP + '\t' + str(TIME) + '\t' + UUID + '\t' + PAGE + '\t' +   ACTION
                print PAGE + '\t' +   ACTION
                
        except Exception:
            continue

PAGE_ID=0
ACTION_ID=1
COLS_CNT = 2

def action_reduce(f):
    table={}
    for line in f:
        #print line
        try:
            cols = line.strip().split('\t')
            #print cols
            if len(cols) < COLS_CNT:
                continue 
            
            #print cols
            PAGE=cols[PAGE_ID]
            ACTION=cols[ACTION_ID]
            
            if PAGE not in table:
                table[PAGE]={}

            if ACTION not in table[PAGE]:
                table[PAGE][ACTION] = 1
            else:
                table[PAGE][ACTION] += 1
            
        except Exception:
            continue
    
    for PAGE in table:
        for ACTION in table[PAGE]:
            print PAGE + '\t' + ACTION + '\t' + repr(table[PAGE][ACTION])

if __name__=="__main__":
    if len(sys.argv) >= 2:
        if sys.argv[1] == "Mapper":
            action_mapper(sys.stdin)
        elif sys.argv[1] == "Reduce":
            action_reduce(sys.stdin)
    else:
        action_reduce(sys.stdin)
