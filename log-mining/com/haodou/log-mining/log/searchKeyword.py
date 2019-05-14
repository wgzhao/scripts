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

searchCnt = 0         #offset=0的搜索
searchPullDown = 1    #下拉产生的搜索，ofset>0
clikAll=  2    #所有点击
clikRid  =  3    #匹配菜谱id的点击
clikRidList   =  4    #食谱列表统计

colCnt = 5 #mapper输出的最小列数

outRidCnt = 20 #输出菜谱列表的个数

searchHead={
"search.getlist":1,
"search.getlistv2":1,
"search.getlistv3":1,
}

#定义所有能返回菜谱列表的方法
getRecipeMethod=set([
'recipe.getcollectlist',
'search.getlist',
'search.getlistv2',
'search.getlistv3',
'recipe.getcollectrecomment',
'rank.getrankview',
'info.getalbuminfo',
'suggest.top',
#'recipephoto.getproducts', #只返回一条食谱
'recipeuser.getuserrecipelist',
'recipephoto.getlist'
])

def searchFollowMapper(f):
    lastU=""
    lastM="_" #最后调用的获取菜谱列表的方法
    KeyWordClick={} #点击次数
    RidList=set([])#搜索返回的食谱列表
    
    #for line in sys.stdin:
    for line in f:
        try:
            cols=line.strip().split("\t")
            #print line
            if len(cols) < column.APP_LOG_COLUMNS:
                continue    
            
            u=column.uuidFirst(cols[1:]) #获得uuid
            para=cols[column.PARA_ID+1] #得到请求参数
            if lastU == "":#第一个用户
                lastU=u
                
            if lastU != u: #下一个用户
                lastU=u
                lastM="_"
                RidList=set([])
    
    
            method=cols[column.METHOD_CID+1]  #获得请求的方法
            if method in getRecipeMethod:
                lastM = method  #最后获得菜谱列表的方法
                RidList=set([])
                
            if method in searchHead: #搜索方法
                keyword=column.getValue(para,"keyword") #搜索的关键字
                #print keyword
                offset=column.getValue(para,"offset")
                #print offset
                if len(keyword) == 0: keyword ="_"
                if len(offset) == 0: offset ="0"
                
                if keyword not in KeyWordClick: #还没统计过的新关键词
                    KeyWordClick[keyword]=[0,0,0,0,{}] #第一个用于统计没下拉的搜索的次数，第二个统计下拉搜索的次数，第三个统计所有可能的点击， 第三个用于统计匹配到菜谱的点击，第五个字典用于菜谱点击统计

                if offset != '0': #搜索下拉
                    KeyWordClick[keyword][searchPullDown] += 1  #统计搜索下拉
                    KeyWordClick[keyword][searchCnt] += 1  #统计第一次搜索
                else:   #第一次搜索
                    KeyWordClick[keyword][searchCnt] += 1  #统计第一次搜索
                
                ret=column2.FuncMap[method](cols[-1])   #获得搜索返回的食谱列表
                RidList=RidList | set(ret['rids'])   #获得搜索返回的食谱列表,搜索过程会连续翻页，产生多次搜索调用，因此合并返回的列表
                #print offset
                
            #产生点击
            if (method.startswith("info.getinfo") or method == "info.getlastestinfo"):
                rid=column.getValue(para,"rid") #获得点击的食谱id
                #print rid
                #搜索次数统计
                if rid in RidList: #点击的菜谱ID在搜索的返回列表中
                    KeyWordClick[keyword][clikAll] += 1 #可能的点击+1
                    KeyWordClick[keyword][clikRid] += 1 #匹配的点击+1        
                    #菜谱点击统计
                    if rid not in KeyWordClick[keyword][clikRidList]: #如果该菜谱从来没有被统计过
                        KeyWordClick[keyword][clikRidList][rid] = 1 ;
                    else:
                        KeyWordClick[keyword][clikRidList][rid] += 1 ; #菜谱点击次数+1
                elif lastM in searchHead  : #如果没有匹配到菜谱，但最后调用的是搜索方法，仍然统计可能的点击次数
                    KeyWordClick[keyword][clikAll] += 1 #可能的点击+1            
                    #菜谱点击统计
                    if rid not in KeyWordClick[keyword][clikRidList]: #如果该菜谱从来没有被统计过
                        KeyWordClick[keyword][clikRidList][rid] = 1 ;
                    else:
                        KeyWordClick[keyword][clikRidList][rid] += 1 ; #菜谱点击次数+1
        except Exception,ex:
            continue    
    for item in KeyWordClick:
        try:
            outstring = item + '\t' + str(KeyWordClick[item][searchCnt]) + '\t' + str(KeyWordClick[item][searchPullDown]) + '\t' + str(KeyWordClick[item][clikAll]) + '\t' + str(KeyWordClick[item][clikRid])#输出关键词统计
            for i in  KeyWordClick[item][clikRidList]: #输出菜谱统计
                outstring +=  '\t'+ i + ':' + str(KeyWordClick[item][clikRidList][i])
        except Exception,ex:
            continue                
        print outstring

def searchFollowReduce(f):
    keyWordClick={} #点击次数
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
            outstring = item + '\t' + str(keyWordClick[item][searchCnt]) + '\t' + str(keyWordClick[item][searchPullDown])  + '\t' + str(keyWordClick[item][clikAll])  + '\t' + str(keyWordClick[item][clikRid])#输出关键词统计
            
            x=keyWordClick[item][clikRidList]
            sorted_rid_cnt = sorted(x.iteritems(), key=lambda x : x[1] ,reverse=True)  #按菜谱点击次数从高到低排序
            iCnt =0
            for i in  sorted_rid_cnt: #输出菜谱统计
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


