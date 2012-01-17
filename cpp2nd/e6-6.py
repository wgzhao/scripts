def ex_66(str):
    '''
    实现string.strip()功能，也就是删除一个字符串的前导和尾缀空格
    实现的思路是，首先把字符串转为list类型。
    而后从第一个字符开始，如果是空格，则删除。一直遇到第一个非空格字符。
    然后从最后开始往前扫描，一直到第一个非空格字符，其他删除。
    转为list的目的是方便删除
    >>> ex_66('  abc  ')
    abc
    >>> ex_66(' a  bc d    ')
    a  bc d
    '''
    strlist=[ char for char in str ]
    
    #每次弹出第一个字符，如果不是空格，则退出循环，并将该字符插入到list第一个位置
    while True:
        char = strlist.pop(0)
        if char != ' ':
            strlist.insert(0,char)
            break
            
    #每次弹出最后一个字符，如果不是空格，则退出循环，并将该字符插入到list后面
    while True:
        char = strlist.pop()
        if char != ' ':
            strlist.append(char)
            break
    
    print ''.join(strlist)
    

    
if __name__ == "__main__":
    from doctest import testmod
    testmod()
