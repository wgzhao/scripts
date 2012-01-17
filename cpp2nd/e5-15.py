#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_515(num1,num2):
    '''
    计算给定的两个数的最大公约数和最小公倍数
    最大公约数采取辗转相除法获得
    http://zh.wikipedia.org/zh-cn/%E8%BC%BE%E8%BD%89%E7%9B%B8%E9%99%A4
    最小公布倍数 = num1 * num2 / 最大公约数
    >>> ex_515(4,6)
    2 12
    >>> ex_515(20,16)
    4 80
    '''
    tn = num1 * num2
    while num1 != num2:
        if num1 < num2:
            num1,num2 = num2,num1
        num1 -= num2
        
    print num1,tn / num1        
    
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()
