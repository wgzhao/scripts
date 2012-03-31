import math
def ex_85(num):
    '''
    打印给出数的所有约束，包括1和本身
    >>> ex_85(18)
    [1, 2, 3, 6, 9, 18]
    >>> ex_85(17)
    [1, 17]
    '''
    b = [1]
    l = [num]
    for n in range(2,int(math.sqrt(num)) + 1):
        if num % n == 0:
            b.append(n)
            l.insert(0,num / n)
    b = b + l
    print b
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()