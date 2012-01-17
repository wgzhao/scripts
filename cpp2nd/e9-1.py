import re
def ex9_1(filename):
    '''
    过滤以#字符作为注释的行，输出过滤后的文本
    '''
    #首先过滤掉#开头的行
    data = [ line for line in open(filename) if line[0] != '#']
    #过滤从#开始一直到行尾的字符
    p = re.compile(r'#.*$')
    result = [ p.sub('',line) for line in data ]
    
    print ''.join(result)
