#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
def ex_55(money):
    '''
    给出美元数，然后算出要多少个硬币
    money的单位是美元
    >>> ex_55(0.82)
    {25: 3, 5: 1, 1: 2}
    >>> ex_55(0.76)
    {25: 3, 1: 1}
     
    '''
    coins=[25,10,5,1]
    dcoins={}
    money *= 100 #convert to cents
    for coin in coins:
        if money == 0:
            return dcoins
        (r,money) = divmod(money,coin)
        if r >0: dcoins[coin] = int(r)
    print  dcoins
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()