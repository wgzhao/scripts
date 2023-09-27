#!/usr/bin/env python3
# take staff-center some table data to production
from sqlalchemy import create_engine
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as parquet
from urllib.parse import quote
import subprocess
import os

tables = [
    't_uniusr_branch_dept_mapping',
    't_uniusr_dept',
    't_uniusr_usr_acquisition',
    't_uniusr_usr_examcourse',
    't_uniusr_usrext',
    't_uniusr_usrgroup',
    't_uniusr_usrgroup_member',
]
tables = ['t_uniusr_usrgroup']

tmpdir = "/tmp/staff"

if not os.path.exists(tmpdir):
    os.mkdir(tmpdir)
hive_basedir = '/mnt/dfs/ods/odsyg'

conn = create_engine(
    "mysql://{}:{}@10.90.70.123:3306/uump_dev?charset=utf8".format(
        'umpuser', 'pyj6xF9Pb6zOkf95'))

for table in tables:
    print("process {}".format(table))
    df = pd.read_sql(table, con=conn)
    # for col in df.columns:
    #     if df[col].dtype == 'datetime64[ns]':
    #         df[col] = df[col].astype('str')
    ptable = pa.Table.from_pandas(df, preserve_index=False)
    print("write orc file.....")
    orc.write_table(ptable, '{}/{}.orc'.format(tmpdir, table),
        compression='LZ4', file_version='0.12')
    try:
        orc.write_table(ptable, '{}/{}.orc'.format(tmpdir, table),
                        compression='LZ4', file_version='0.12')
    except Exception as e:
        print("failed, skip it: ", e)
        
        continue
# print("copy to remote host")
# cmd = 'scp -r {} hive@infa01:/tmp'.format(tmpdir)
# print(subprocess.getoutput(cmd))
# cmd = '/Users/wgzhao/bin/orc-tools convert /tmp/aaa.json'
# print(subprocess.getoutput(cmd))
