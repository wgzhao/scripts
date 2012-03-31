def ex9_19(byte,cnt,filesize):
    slist = []
    i = 0
    while i < filesize - cnt:
        b = randint(0,255)
        if b != byte:
            slist.append(chr(b))
        i += 1
        
    for i in range(cnt):
        pos = randint(0,len(slist))
        slist.insert(pos,chr(byte)) #insert into randomly
        
    f = open('/tmp/test','wb')
    f.write(''.join(slist))
    f.close()
