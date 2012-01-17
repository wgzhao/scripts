#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import math
def ex_84(num):
    '''
    判断给定的数是否是素数，如果是，打印True，否则打印False
    判断的方法就是看num能否被[2,int(sqrt(num)]中的任意数整除，如果是，则不是素数，否则是
    
    >>> ex_84(3)
    True
    >>> ex_84(9)
    False
    >>> ex_84(247)
    False
    '''
    flag = True
    for d in range(2,int(math.sqrt(num)) + 1 ):
        if num % d == 0:
            flag = False
            break
    print flag
        

if __name__ == "__main__":
    import doctest
    doctest.testmod()
