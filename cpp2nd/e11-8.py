def ex11_8(ylist):
    '''
    使用filter()函数来计算给定的年份列表，返回闰年列表
    >>> ex11_8([1900,1906,2000,2008,2010])
    [2000, 2008]
    [2000, 2008]
    '''
    def leap_year(year):
        if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
            return True
        else:
            False
    result = filter(leap_year,ylist)
    print result 
    
    #列表解析
    result = [ y for y in ylist if leap_year(y) ]
    print result
if __name__ == '__main__':
    import doctest
    doctest.testmod()