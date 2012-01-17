def mult(x,y): 
    return x * y
    
def fcalc(n):
    '''
    use reduce 
    '''
    return reduce(mult,range(1,n + 1))
    
def lcalc(n):
    '''
    use anonymouse function
    '''
    return reduce(lambda x,y:x*y,range(1,n+1))
    
def rec(n):
    '''
    use recusive
    '''
    if n == 0 or n == 1: return 1
    else:
        return n * rec(n-1)  
        
def icalc(n):
    '''
    use iteration
    '''
    result = 1
    index =1
    while index < =n:
        result = result * index
        index +=1
    return result
    
def ex11_13():
    '''
    '''
    print 'use iteration to  calculate 10!'
    (r,t) = ex11_12(icalc,arg=20)
    print 'result = %d cosumer time = %f' % (r,t)   
    
    print 'use mult function to  calculate 10!'
    (r,t) = ex11_12(fcalc,arg=20)
    print 'result = %d cosumer time = %f' % (r,t)
    
    print 'use lambda function to calculate 10!'
    (r,t) = ex11_12(lcalc,arg=20)
    print 'result = %d cosumer time = %f' % (r,t)
    
    print 'calculate 10! by recursed'
    (r,t) = ex11_12(rec,arg=20)
    print 'result = %d cosumer time = %f' % (r,t)

if __name__ == '__main__':
    ex11_13()
