def ex9_6(f1,f2):

    '''

    files compare,print the first difference between two files

    '''

    f1size = os.stat(f1).st_size

    f2size = os.stat(f2).st_size

    if f1size > f2size:

        f1,f2 = f2,f1

        f1size,f2size = f2size,f1size


    fd1 = open(f1,'r')

    fd2 = open(f2,'r')

    row = 1

    col = 1

    pos = 0

    while pos < f1size:

        ch1 = fd1.read(1)

        ch2 = fd2.read(1)

        if ch1 != ch2:

            print 'first different at (row,line) = (%d,%d)' %(row,col)

            break

        elif ch1 == '\n':

            row +=1 #carrie line

            col = 1 #new line

        else:

            col +=1

        pos +=1

    else:

        if f1size != f2size:

            print 'first different at (row,line) = (%d,%d)' %(row,col)

        else:

            print 'same file'

