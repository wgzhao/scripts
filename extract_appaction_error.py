#!/usr/bin/env python
import sys
import json
from io import StringIO
for line in sys.stdin:
    io = StringIO(line)
    data = json.load(io)
    print(list(data.keys()))
    for d in data['ext']:
        if d['action'] == 'A3001':
            #print data['user_ip'],data['device_time'],data['server_time'],
            print(data['b'])
            #print d['action'],d['view'],d['crash'],d['page'],d['time']
    