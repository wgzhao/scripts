#!/usr/bin/env python3
"""
simple but fast dump dbf to csv with multi-process method
"""
import sys
import os
import time
import struct
from multiprocessing import Pool, cpu_count
import argparse
from tempfile import mkdtemp

def get_meta(dbf, codec='GBK'):
    fp = open(dbf, 'rb')
    meta = {}
    meta['numrec'], meta['lenheader'] = struct.unpack('<xxxxLH22x', fp.read(32))
    numfields = (meta['lenheader'] - 33) // 32

    # The first field is always a one byte deletion flag
    fields = [('DeletionFlag', 'C', 1),]
    for fieldno in range(numfields):
        name, typ, size = struct.unpack('<11sc4xB15x', fp.read(32))
        # eliminate NUL bytes from name string  
        name = name.strip(b'\x00')        
        fields.append((name.decode(codec), typ.decode(codec), size))
    # Get the names only for DataFrame generation, skip delete flag
    meta['fields'] = fields
    columns = [f[0] for f in fields[1:]]
    meta['columns'] =  columns
    terminator = fp.read(1)
    assert terminator == b'\r'
    
    # Make a format string for extracting the data. In version 5 DBF, all
    # fields are some sort of structured string
    meta['fmt'] = ''.join(['{:d}s'.format(fieldinfo[2]) for 
                        fieldinfo in fields])
    meta['fmtsiz'] = struct.calcsize(meta['fmt'])
    meta['offset'] = 32 + 32 * numfields + 1
    return meta

def dbf2csv(infile, outfile, offset, num, meta, sep=',', codec='GBK'):
    # Escape quotes, set by indiviual runners
    #self._esc = None
    # Reading as binary so bytes will always be returned
    #print(f"task #{os.getpid()} is running")
    fp = open(infile, 'rb')
    fp.seek(meta['offset'] + offset)
    fw = open(outfile, 'w', encoding='UTF8')
    for _ in range(num):
        try:
            record = struct.unpack(meta['fmt'], fp.read(meta['fmtsiz']))
            if record[0] != b' ':
                continue
            result = []
            for idx, value in enumerate(record):
                name, typ, size = meta['fields'][idx]
                if name == 'DeletionFlag':
                    continue
                value = value.decode(codec).strip()
                if value == '-.---':
                    value = ''
                result.append(value)
            fw.write(sep.join(result) + '\n')
        except(struct.error):
            break
        except(UnicodeDecodeError):
            print(f"failed to decode with {codec}: {value}")
    fw.close()
    fp.close()
    #print(f"task #{os.getpid()} has finished")
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="fast dbf to csv dump util using multi process method")
    parser.add_argument('-i','--infile', dest='infile', help="dbf file",  required=True)
    parser.add_argument('-o','-outfile', dest='outfile', help='output file, using output.csv if not specify', default='output.csv')
    parser.add_argument('-s','--sep', help="fields separator, the defaults is comma(,)", default=',')
    parser.add_argument('-e','--codec', dest='codec', help="dbf file encoding, GBK is the default", default='GBK')
    parser.add_argument('-H','--header', dest='header', action='store_true',help="weather include header or not, default is false", default=False)
    args = parser.parse_args()
    btime = time.time()
    meta = get_meta(args.infile)
    pool = Pool(cpu_count() + 2)
    num_per_task=100000
    tasks = meta['numrec'] // num_per_task + 1
    jobs = []
    tmpdir = mkdtemp()
    for i in range(tasks):
        offset = i*meta['fmtsiz']*num_per_task
        fname = f"{tmpdir}{os.sep}{i}.txt"
        job = pool.apply_async(dbf2csv, args=(args.infile, fname, offset, num_per_task, meta, args.sep))
        jobs.append(job)
    for idx, job in enumerate(jobs):
        job.get()
        print("{}% compeleted".format(round((idx+1) * 100/ tasks),2))
    pool.close()
    pool.join()
    if args.header:
        open(args.outfile, 'w').write(args.sep.join(meta['columns']) + '\n')
        os.system(f"cat {tmpdir}{os.sep}* >> {args.outfile}")
    else:
        os.system(f"cat {tmpdir}{os.sep}* > {args.outfile}")
    os.system(f"rm -rf {tmpdir}")
    print(f"taken time {time.time() - btime}s")
