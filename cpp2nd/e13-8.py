class MyStack:
    '''
    模拟堆栈(stack)的实现
    这里采用列表(list)数据类型实现
    '''
    def __init__(self):
        self.st = []
    
    def isempty(self):
        '''
        判断是否为空,如果为空，返回真
        '''
        return len(self.st) == 0
    
    def push(self,data):
        '''
        压栈，数据应该在最前面
        '''
        self.st.insert(0,data)
    
    def pop(self):
        '''
        出栈
        '''
        if not self.isempty():
            if hasattr(list,'pop'):
                return self.st.pop(0)
            else:
                #实现自己的pop方法
                d = self.st[0]
                del sefl.st[0]
                return d
        else:
            print 'stack is empty'
    
    def peek(self):
        '''
        获得栈顶数据
        '''
        if not self.isempty():
            return self.st[0]
        else:
            print 'stack is emmpty'