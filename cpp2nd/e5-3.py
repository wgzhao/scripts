#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
__Author__ = 'wgzhao, wgzhao##gmail.com'
'''
Core Python Programming 2nd exercise
chapter 5
'''
def ex_53(score):
    '''
    输入分数，根据规律输出对应的成绩水平
    A: 90–100
    B: 80–89
    C: 70–79
    D: 60–69
    F: <60
    >>> ex_53(80)
    B
    >>> ex_53(75)
    C
    '''
    if score >=70 and score < = 79:
        print 'C'
    elif score >=80 and score < =89:
        print 'B'
    elif 60 <= score <= 69:
        print 'D'
    elif 90 <= score <=100:
        print 'A'
    else:
        print 'F'

if __name__ == '__main__':
    import doctest
    doctest.testmod()