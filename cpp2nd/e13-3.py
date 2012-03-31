class MoneyFmt:
    
    def __init__(self,value = 0.0,d='-'):
        self.value = value
        self.d = d
    
    def __str__(self):
        '''
        带分隔符打印金额
        '''
        val = '$'
        if self.value >= 0:
           r = str(self.value)
        else:
            r = str(self.value)[1:] #去掉负数符号
            
        if '.' in r:
            (r,d) = r.split('.') #如果有小数，分离小数
        
        r = list(r) 
        pos = len(r) -1
        cnt = 3
        while pos >= cnt:
            r.insert(pos - cnt + 1,',') #每隔3位，加入分隔符
            cnt *=2
                
        val = val + ''.join(r)
        
        #如果有小数，追加到字符串后面
        try:
            val = val + '.' + d
        except:
            pass
            
        if self.value < 0:  #增加表示负数的符号，可以由用户确定，目前只有两种，或者是-，或者是<>
            if self.d == '-':
                return '-' + val
            else:
                return '< ' + val + '>'
        return val
            
        
    def __repr__(self):
        return `self.value`
        
    
    def __nonzero__(self):
        
        return self.value > 0
    
    def update(self,value = None):
        self.value = value
