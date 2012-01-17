def ex9_3():
    '''
    print file's total number of lines
    '''
    filename = raw_input('pls enter your file:')
    
    x = 0
    #DO NOT use len([line for line in open(filename) ]) to reduce occupation of memory
    print len([‘x' for line in  open(filename) ]),filename
