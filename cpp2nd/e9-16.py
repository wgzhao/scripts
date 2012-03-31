def ex9_16(filename):
    '''
    将文件处理为每行长度不大于80的段落
    但不能出现单词截断的情况
    基本思路是：
    首先去掉原来的换行标记，而后以空格为分隔符，将单词转换成列表
    然后开始分隔
    '''
    print filename
    lstr = [line.rstrip() for line in open(filename)]
    index = 0
    result= []
    while index < len(lstr):
        if len(lstr[index]) <=80:
            result.append(lstr[index])
        else:
            tmpstr = lstr[index]
            pos = 79
            while tmpstr[pos] != ' ' and tmpstr:
                pos -=1
            result.append(tmpstr[:pos]) #get the string that less than 80
            if index +1 < len(lstr):
                lstr[index + 1] = tmpstr[pos:] + lstr[index + 1] #move result string to next line
            else:
                lstr.append(tmpstr[pos:])   #add a new line
        index +=1
    
    print '\n'.join(result)
