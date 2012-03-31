#!/usr/bin/env python 
# -*- encoding: utf-8 -*-
def ex_86(num):
    '''
    求解给定数的素因素，以列表方式打印
    >>> ex_86(20)
    [2, 2, 5]
    >>> ex_86(35)
    [5, 7]
    >>> ex_86(100)
    [2, 2, 5, 5]
    '''
    def isPrime(x):
        for i in range(2,int(math.sqrt(x)) +1 ):
            if x % i == 0:
                return False
        return True
        
    factorlist = [x for x in range(2,int(num / 2) + 1) if isPrime(x) ]
    result=[]
    while not num in factorlist:
        for i in [l for  l in factorlist if l < = num ]: #减少循环次数
                if num % i == 0:
                    num = num / i
                    result.append(i)
                    break
    result.append(num)                
    print result
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()