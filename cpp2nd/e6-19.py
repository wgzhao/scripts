#!/usr/bin/env python
# -*- encoding: utf-8 -*-
def ex_619(num=100,cols=3,direction='h'):
    '''
    根据用户指定的项数量，列数已经排版方向，排版出用户的要求。
    比如：
    果你传入 100 个项并定义 3 列输出,按照需要的模式显示这些数据.这种情况下,应
    该是两列显示 33 个项,最后一列显示 34 个
    direction接受两个选项h和v分别表示水平排列和垂直排列
    如果是顺序排列，非常容易，但是如果是垂直排列，则难点在于如何控制打印
   
    '''
    order = range(1,num+1)
    nums_p_col = int(round(num / float(cols))) #计算出每列的元素个数

    pdict = {}
    #按照每列的个数，生成字典
    for i in range(cols):
        index  = 0
        while index < nums_p_col and order:
            if i in pdict:
                pdict[i].append(order.pop(0))
            else:
                pdict[i] = [order.pop(0)]
            index +=1
    
    if order:
        #如果order还有元素，表示最后一列是长列，在垂直打印时，以此列为准
        pdict[cols -1 ] += order
        leng = len(pdict[cols - 1])
    else:
        #最后一列是短列
        leng = len(pdict[0])
        
        
    if direction == 'h':
        #水平打印，只需要把pdict的每个元素打印出来就好了
        for (k,v) in pdict.items():
            for x in v:
                print x,
            print 
    else:
       #垂直打印，则需要每次打印一个pdict项的元素，同时要考虑到长短列
       index  = 0    
       while index < leng:
           col = ''
           for i in range(cols):
               try:
                   item = str(pdict[i].pop(0))
               except:
                   #用空格增补已经没有元素的列
                   item = ' '
               finally:
                    col += item + ' '
           print col
           index +=1
                
    
if __name__ == "__main__":
    ex_619(7,3,'h')
    ex_619(7,3,'v')
    ex_619(5,3,'h')
    ex_619(5,3,'v')
