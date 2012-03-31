#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_510(f):
    '''
    给出华氏问题，计算出摄氏温度
    计算公式：
    C = (F - 32 )  * (5 / 9)
    >>> ex_510(80)
    26.7
    >>> ex_510(61)
    16.1
    '''
    print round(((f - 32 ) * 5 / 9.0),1) #取一个小数
    
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()