#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_58(shape,item):
    '''根据给定的形状，分别计算表面积和体积
    形状有square,cube,circle,ball
    >>> ex_58('squre',2)
    area: 4 volume: 0
    >>> ex_58('cube',3)
    area: 54 volume: 27.0
    >>> ex_58('ball',3)
    area: 113.04 volume: 113.04
    '''
    from math import pow
    area = {}
    volume = {}
    if shape == 'squre':
        area = item ** 2
        volume = 0
    elif shape == 'cube':
        area = 6 * item ** 2
        volume = pow(item,3)
    elif shape == 'circle':
        area = 3.14 * item ** 2
        volume = 0
    elif shape == 'ball':
        area = 4 * 3.14 * item ** 2
        volume = 4 * 3.14 * pow(item,3) / 3.0
    print 'area: %s volume: %s' % (str(area),str(volume))
   
    
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()