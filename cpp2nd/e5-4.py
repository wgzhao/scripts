#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_54(year):
    '''
    判断给出的年份是否为闰年
    >>> ex_54(1992)
    1992 is a leap year
    >>> ex_54(2000)
    2000 is a leap year
    >>> ex_54(1967)
    1967 is NOT a leap year
    '''
    if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
        print "%d is a leap year" % year
    else:
        print "%d is NOT a leap year" % year

if __name__ == '__main__':
    import doctest
    doctest.testmod()