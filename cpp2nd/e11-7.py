def ex11_7(list1,list2):
    '''
    用 map() 进 行 函 数 式 编 程 。 给 定 一 对 同 一 大 小 的 列 表 , 如 [1 , 2 , 3] 和
    ['abc','def','ghi',....],将两个标归并为一个由每个列表元素组成的元组的单一的表,以使我
    们的结果看起来像这样:{[(1, 'abc'), (2, 'def'), (3, 'ghi'), ...}.不要使用内置的zip函数
    >>> ex11_7([1,2,3],['abc','def','ghi'])
    [(1, 'abc'), (2, 'def'), (3, 'ghi')]
    '''
    if len(list1) != len(list2):
        print 'Error'
    else:
        result = []
        while list1:
            result.append((list1.pop(0),list2.pop(0)))
    print result
if __name__ == '__main__':
    import doctest
    doctest.testmod()