def ex9_2():
    '''
    print the first N lines of file
    '''
    filename = raw_input('pls enter your filename:')
    num = int(raw_input('how many lines do you want print:'))
    f = open(filename)
    index = 0
    while index < num:
        print f.next(),
        index +=1
    f.close()
