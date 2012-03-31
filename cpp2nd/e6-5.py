def ex_65(str):
    '''
    1) print each char of strings by order ,then display by reversed order
    2) check a string is palindrome or not
    3) generate a palindrome string
    >>> ex_65('acdeb')
    a c d e b
    b e d c a
    No
    a c d e b e d c a
    >>> ex_65('abcba')
    a b c b a
    a b c b a
    Yes
    a b c b a
    '''
    for char in str:
        print char,
    print
    num=len(str) - 1
    while num >= 0:
        print str[num],
        num -= 1
    print
    epos = len(str) -1
    bpos =  0
    while epos != bpos:
        if str[epos] != str[bpos]:
            print 'No'
            break
        epos -= 1
        bpos += 1
            
    if epos == bpos:
        print 'Yes'
        for char in str:
            print char,
    else:
        #generate palindrome
        newstr = str[:]
        
        for i in range(len(str) -2,-1,-1):
            newstr += str[i]
        for char in newstr:
            print char,
    
    
    
if __name__ == "__main__":
    from doctest import testmod
    testmod()
