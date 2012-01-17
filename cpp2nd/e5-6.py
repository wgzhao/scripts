#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_56(str):
    '''
    给定运算字符串，然后输出结果
    字符串格式是
    N1 OP N2
    字符之间只允许有一个空格，不检查空格个数
    >>> ex_56('5 * 6')
    30
    >>> ex_56('2 ** 3')
    8
    >>> ex_56('7 % 3')
    1
    >>> ex_56('2.5 + 3')
    5.5
    >>> ex_56('1.2 - 3.4')
    -2.2
    >>> ex_56('2 ^ 4')
    illegal operator
    '''
    (num1,op,num2) = str.split(' ')
    if '.' in num1:
        num1 = float(num1)
    else:
        num1 = int(num1)
    if '.' in num2:
        num2 = float(num2)
    else:
        num2 = int(num2)
    if op == '+':
        print num1 + num2
    elif op == '-':
        print num1 - num2
    elif op == '*':
        print num1 * num2
    elif op == '/':
        print num1 / num2
    elif op == '%':
        print num1 % num2
    elif op == '**':
        print num1 ** num2
    else:
        print 'illegal operator'    
        
    
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()