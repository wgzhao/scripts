# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 16:02:07 2014

@author: Administrator
"""

fname=r'e:\work\applog1\searchKeyword.txt'
text=open(fname,'r').read()
lines=text.split('\n')
#tmp={}
tmp=[]
#linesL=[(line.strip().split('\t')) for line in lines]
for line in lines:
    cols=line.strip().split('\t')
    if len(cols)<2:continue
    #tmp[cols[0]]=[int(cols[1]),line]
    tmp.append((int(cols[1]),line))
    
#lines_sorted=sorted(tmp.items(),key=lambda tmp:tmp[1],reverse=True)
lines_sorted=sorted(tmp,reverse=True)

fout=open(r'e:\work\applog1\searchKeywordSorted.txt',"w")
for line in lines_sorted:
    fout.write(line[1]+'\n')
fout.close()