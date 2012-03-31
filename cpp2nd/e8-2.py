def ex_82(f,t,i):
    '''
    循环. 编写一个程序, 让用户输入三个数字: (f)rom, (t)o, 和 (i)ncrement . 以 i
    为步长, 从 f 计数到 t , 包括 f 和 t 
    需要判断是否会构成无限循环
    示例：
    >>> ex_82(2,26,4)
    2 6 10 14 18 22 26
    >>> ex_82(1,13,2)
    1 3 5 7 9 11 13
    >>> ex_82(8,0,-2)
    8 6 4 2 0
    >>> ex_82(-1,-8,2)
    infinity loop
    >>> ex_82(8,1,2)
    infinity loop
    '''
    if f < =t and i >0:
        while f < = t:
            print f,
            f = f + i
    elif f >= t and i< 0:
        while f >=t:
            print f,
            f = f + i
    else:
        print 'infinity loop'

            

if __name__ == "__main__":
    import doctest
    doctest.testmod()