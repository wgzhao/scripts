# -*- coding: gbk -*-
"""
Created on Wed Sep 24 15:37:29 2014

@author: Yanghong
"""

import sys
import operator

sys.path.append("./")
sys.path.append("../")
sys.path.append("../abtest")

import column
import column2

searchCnt = 0         #offset=0������
searchPullDown = 1    #����������������ofset>0
clikAll=  2    #���е��
clikRid  =  3    #ƥ�����id�ĵ��
clikRidList   =  4    #ʳ���б�ͳ��

colCnt = 5 #mapper�������С����

outRidCnt = 20 #��������б�ĸ���

searchHead={
"search.getlist":1,
"search.getlistv2":1,
"search.getlistv3":1,
}

#���������ܷ��ز����б�ķ���
getRecipeMethod=set([
'recipe.getcollectlist',
'search.getlist',
'search.getlistv2',
'search.getlistv3',
'recipe.getcollectrecomment',
'rank.getrankview',
'info.getalbuminfo',
'suggest.top',
#'recipephoto.getproducts', #ֻ����һ��ʳ��
'recipeuser.getuserrecipelist',
'recipephoto.getlist'
])

def searchFollowMapper(f):
    lastU=""
    lastM="_" #�����õĻ�ȡ�����б�ķ���
    KeyWordClick={} #�������
    RidList=set([])#�������ص�ʳ���б�
    
    #for line in sys.stdin:
    for line in f:
        try:
            cols=line.strip().split("\t")
            #print line
            if len(cols) < column.APP_LOG_COLUMNS:
                continue    
            
            u=column.uuidFirst(cols[1:]) #���uuid
            para=cols[column.PARA_ID+1] #�õ��������
            if lastU == "":#��һ���û�
                lastU=u
                
            if lastU != u: #��һ���û�
                lastU=u
                lastM="_"
                RidList=set([])
    
    
            method=cols[column.METHOD_CID+1]  #�������ķ���
            if method in getRecipeMethod:
                lastM = method  #����ò����б�ķ���
                RidList=set([])
                
            if method in searchHead: #��������
                keyword=column.getValue(para,"keyword") #�����Ĺؼ���
                #print keyword
                offset=column.getValue(para,"offset")
                #print offset
                if len(keyword) == 0: keyword ="_"
                if len(offset) == 0: offset ="0"
                
                if keyword not in KeyWordClick: #��ûͳ�ƹ����¹ؼ���
                    KeyWordClick[keyword]=[0,0,0,0,{}] #��һ������ͳ��û�����������Ĵ������ڶ���ͳ�����������Ĵ�����������ͳ�����п��ܵĵ���� ����������ͳ��ƥ�䵽���׵ĵ����������ֵ����ڲ��׵��ͳ��

                if offset != '0': #��������
                    KeyWordClick[keyword][searchPullDown] += 1  #ͳ����������
                    KeyWordClick[keyword][searchCnt] += 1  #ͳ�Ƶ�һ������
                else:   #��һ������
                    KeyWordClick[keyword][searchCnt] += 1  #ͳ�Ƶ�һ������
                
                ret=column2.FuncMap[method](cols[-1])   #����������ص�ʳ���б�
                RidList=RidList | set(ret['rids'])   #����������ص�ʳ���б�,�������̻�������ҳ����������������ã���˺ϲ����ص��б�
                #print offset
                
            #�������
            if (method.startswith("info.getinfo") or method == "info.getlastestinfo"):
                rid=column.getValue(para,"rid") #��õ����ʳ��id
                #print rid
                #��������ͳ��
                if rid in RidList: #����Ĳ���ID�������ķ����б���
                    KeyWordClick[keyword][clikAll] += 1 #���ܵĵ��+1
                    KeyWordClick[keyword][clikRid] += 1 #ƥ��ĵ��+1        
                    #���׵��ͳ��
                    if rid not in KeyWordClick[keyword][clikRidList]: #����ò��״���û�б�ͳ�ƹ�
                        KeyWordClick[keyword][clikRidList][rid] = 1 ;
                    else:
                        KeyWordClick[keyword][clikRidList][rid] += 1 ; #���׵������+1
                elif lastM in searchHead  : #���û��ƥ�䵽���ף��������õ���������������Ȼͳ�ƿ��ܵĵ������
                    KeyWordClick[keyword][clikAll] += 1 #���ܵĵ��+1            
                    #���׵��ͳ��
                    if rid not in KeyWordClick[keyword][clikRidList]: #����ò��״���û�б�ͳ�ƹ�
                        KeyWordClick[keyword][clikRidList][rid] = 1 ;
                    else:
                        KeyWordClick[keyword][clikRidList][rid] += 1 ; #���׵������+1
        except Exception,ex:
            continue    
    for item in KeyWordClick:
        try:
            outstring = item + '\t' + str(KeyWordClick[item][searchCnt]) + '\t' + str(KeyWordClick[item][searchPullDown]) + '\t' + str(KeyWordClick[item][clikAll]) + '\t' + str(KeyWordClick[item][clikRid])#����ؼ���ͳ��
            for i in  KeyWordClick[item][clikRidList]: #�������ͳ��
                outstring +=  '\t'+ i + ':' + str(KeyWordClick[item][clikRidList][i])
        except Exception,ex:
            continue                
        print outstring

def searchFollowReduce(f):
    keyWordClick={} #�������
    for line in f:
        try:
            cols=line.strip().split('\t')
            cols_len = len(cols)
            if cols_len < colCnt:
                continue 
            keyword = cols[0]
            if keyword not in keyWordClick:
                keyWordClick[keyword]=[0,0,0,0,{}]
            cnt0 = int(cols[searchCnt +1])
            cnt1 = int(cols[searchPullDown+1])
            cnt2 = int(cols[clikAll+1])
            cnt3 = int(cols[clikRid+1])
            keyWordClick[keyword][searchCnt] +=cnt0
            keyWordClick[keyword][searchPullDown] +=cnt1
            keyWordClick[keyword][clikAll] +=cnt2
            keyWordClick[keyword][clikRid] +=cnt3
            
            if cols_len >colCnt:
                for i in range(colCnt,cols_len):
                    item=cols[i].strip().split(":")
                    if len(item)<2: continue
                    rid = item[0]
                    rid_cnt = int(item[1])
                    if rid not in keyWordClick[keyword][clikRidList]:
                         keyWordClick[keyword][clikRidList][rid] = rid_cnt ;
                    else:
                         keyWordClick[keyword][clikRidList][rid] += rid_cnt ;
        except Exception,ex:
            #sys.stderr.write('error:'+ex+line)
            continue

    for item in keyWordClick:
        try:
            outstring = item + '\t' + str(keyWordClick[item][searchCnt]) + '\t' + str(keyWordClick[item][searchPullDown])  + '\t' + str(keyWordClick[item][clikAll])  + '\t' + str(keyWordClick[item][clikRid])#����ؼ���ͳ��
            
            x=keyWordClick[item][clikRidList]
            sorted_rid_cnt = sorted(x.iteritems(), key=lambda x : x[1] ,reverse=True)  #�����׵�������Ӹߵ�������
            iCnt =0
            for i in  sorted_rid_cnt: #�������ͳ��
                if iCnt > outRidCnt: break
                #outstring +=  '\t'+ i + ':' + str(keyWordClick[item][3][i])
                outstring +=  '\t'+ i[0] + ':' + str(i[1])
                iCnt += 1
        except Exception,ex:
            #sys.stderr.write('error:'+ex+line)
            continue
        print outstring

            

if __name__=="__main__":
    #searchFollow()
    if len(sys.argv) >= 2:
        if sys.argv[1] == "Mapper":
            searchFollowMapper(sys.stdin)
        elif sys.argv[1] == "Reduce":
            searchFollowReduce(sys.stdin)
    else:
        searchFollowReduce(sys.stdin)    


