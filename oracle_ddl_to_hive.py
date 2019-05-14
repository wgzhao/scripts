#!/usr/local/bin/python3
"""
convert Oracle DDL SQL into hive sql
here is a sample of oracle ddl sql

/*==============================================================*/
/* Table: GZ_TTMP_H_GZB                                         */
/*==============================================================*/
create table EAM.GZ_TTMP_H_GZB 
(
   L_BH                 NUMBER(8),
   VC_KMDM              VARCHAR2(400),
   VC_KMMC              VARCHAR2(128),
   L_SL                 NUMBER(19,4),
   EN_DWCB              NUMBER(19,4),
   EN_CB                NUMBER(30,6),
   EN_CBZJZ             NUMBER(19,4),
   EN_HQJZ              NUMBER(19,6),
   EN_SZ                NUMBER(30,6),
   EN_SZZJZ             NUMBER(19,4),
   EN_GZZZ              NUMBER(19,4),
   VC_TPXX              VARCHAR2(32),
   L_ZTBH               NUMBER(4),
   D_YWRQ               VARCHAR2(10),
   VC_CODE_HS           VARCHAR2(32),
   L_KIND               NUMBER(2,1),
   VC_JSBZ              VARCHAR2(3),
   EN_EXCH              NUMBER(23,10),
   EN_WBCB              NUMBER(19,4),
   EN_WBHQ              NUMBER(19,4),
   EN_WBSZ              NUMBER(19,4),
   L_ZQNM               NUMBER(8),
   FLAG                 VARCHAR2(10),
   VC_BZW               VARCHAR2(20)
)
pctfree 10
initrans 1
storage
(
    initial 64K
    next 1024K
    minextents 1
    maxextents unlimited
)
tablespace TS_EAM
logging
 nocompress
 monitoring
 noparallel
/

comment on column EAM.GZ_TTMP_H_GZB.L_BH is
'编号'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_KMDM is
'科目代码'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_KMMC is
'科目名称'
/

comment on column EAM.GZ_TTMP_H_GZB.L_SL is
'数量'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_DWCB is
'单位成本'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_CB is
'成本'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_CBZJZ is
'成本占净值'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_HQJZ is
'行情价值'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_SZ is
'市值'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_SZZJZ is
'市值占净值'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_GZZZ is
'估值增值'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_TPXX is
'停牌信息'
/

comment on column EAM.GZ_TTMP_H_GZB.L_ZTBH is
'帐套编号'
/

comment on column EAM.GZ_TTMP_H_GZB.D_YWRQ is
'业务日期'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_CODE_HS is
'科目属性'
/

comment on column EAM.GZ_TTMP_H_GZB.L_KIND is
'借贷方向'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_JSBZ is
'结算币种'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_EXCH is
'汇率'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_WBCB is
'外币成本'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_WBHQ is
'外币行情'
/

comment on column EAM.GZ_TTMP_H_GZB.EN_WBSZ is
'外币市值'
/

comment on column EAM.GZ_TTMP_H_GZB.L_ZQNM is
'证券内码'
/

comment on column EAM.GZ_TTMP_H_GZB.VC_BZW is
'标志位'
/

it will convert the following sql:
create table eam.gz_ttmp_h_gzb (
    l_bh decimal(8) comment '编号',
    vc_kmdm varchar(400) comment '科目代码',
    vc_kmmc varchar(128) comment '科目名称',
    l_sl decimal(19,4) comment '数量',
    en_dwcb decimal(19,4) comment '单位成本',
    en_cb decimal(30,6) comment '成本',
    en_cbzjz decimal(19,4) comment '成本占净值',
    en_hqjz decimal(19,6) comment '行情价值',
    en_sz decimal(30,6) comment '市值',
    en_szzjz decimal(19,4) comment '市值占净值',
    en_gzzz decimal(19,4) comment '估值增值',
    vc_tpxx varchar(32) comment '停牌信息',
    l_ztbh decimal(4) comment '帐套编号',
    d_ywrq varchar(10) comment '业务日期',
    vc_code_hs varchar(32) comment '科目属性',
    l_kind decimal(2,1) comment '借贷方向',
    vc_jsbz varchar(3) comment '结算币种',
    en_exch decimal(23,10) comment '汇率',
    en_wbcb decimal(19,4) comment '外币成本',
    en_wbhq decimal(19,4) comment '外币行情',
    en_wbsz decimal(19,4) comment '外币市值',
    l_zqnm decimal(8) comment '证券内码',
    flag varchar(10),
    vc_bzw varchar(20) comment '标志位'
);
"""
from ddlparse.ddlparse import DdlParse
import re
import sys

# 存储转换过来的表定义，包括字段名称，类型和注释（如果有）
table_result = {}
# 存储表注释（如果有）
table_comment = {}

# 需要转换类型
type_mapping = {'VARCHAR2':'varchar','NUMBER':'decimal','CLOB':'varbinary','BLOB':'varbinary'}

# 字段注释提取正则表达式
p_col = re.compile(r"^comment\s+on\s+column\s+(.*?)\.(.*?)\.(.*?)\s+is\s+(.*?)$")
# 表注释提取正则表达式
p_tbl = re.compile(r"^comment\s+on\s+table\s+(.*?)\.(.*?)\s+is\s+(.*?)$")

def combine_type(value):
  """
  类型转换以及类型长度合成
  """
  re_type = ''
  if value.data_type in type_mapping:
    re_type += type_mapping[value.data_type]
  else:
    re_type += value.data_type.lower()
  if value.scale:
    re_type += '({},{})'.format(value.precision, value.scale)
  elif value.length:
    re_type += '({})'.format(value.length)
  return re_type

def extract_table(ddl):
  t = DdlParse().parse(ddl, source_database=DdlParse.DATABASE.oracle)
  schema = t.schema.lower()
  tbl = t.name.lower()
  if schema in table_result:
    table_result[schema][tbl] = []
  else:
    table_result[schema] = {tbl:[]}

  for col in t.columns.values():
    table_result[schema][tbl].append({'colname':col.name,'type':combine_type(col)})
    
def extract_comment(item, flag='column'):
  item = item.lower().replace('\n',' ')
    
  if flag == 'column':
    m = p_col.search(item)
    schema, tbl, colname, comment = m.groups()
    # find col
    for idx, k in enumerate(table_result[schema][tbl]):
      if k['colname'] == colname:
        table_result[schema][tbl][idx]['comment'] = comment
  elif flag == 'table':
    m = p_tbl.search(item)
    schema, tbl, comment = m.groups()
    if schema in table_comment:
      table_comment[schema][tbl] = comment
    else:
      table_comment[schema] = {tbl:comment}
  
def convert(ddl_sql):
  for item in ddl_sql:
    item = item.strip().lower()
    if item.startswith('create table'):
      extract_table(item)
    elif item.startswith('comment on column'):
      extract_comment(item, flag='column')
    elif item.startswith('comment on table'):
      extract_comment(item, flag='table')

  # create hive-compatiable ddl
  ddl_str = ''
  for db in table_result:
    for tbl in table_result[db]:
      tbl_ddl = 'create table {}.{} (\n'.format(db,tbl)
      for col in table_result[db][tbl]:
        if 'comment' in col:
          tbl_ddl += "    {} {} comment {},\n".format(col['colname'],col['type'], col['comment'])
        else:
          tbl_ddl += "    {} {},\n".format(col['colname'], col['type'])
      if tbl in table_comment[db]:
        ddl_str += tbl_ddl[:-2] + '\n)comment {};\n\n'.format(table_comment[db][tbl])
      else:
        ddl_str += tbl_ddl[:-2] + '\n);\n\n'
  return ddl_str

if __name__ == '__main__':
  if len(sys.argv) < 3:
    print('Usage: {} <oracle_ddl_file> <output_file> [delimiter]'.format(sys.argv[0]))
    sys.exit(1)
  filepath = sys.argv[1]
  outfile = sys.argv[2]
  if len(sys.argv) == 4:
    delimiter = sys.argv[3]
  else:
    delimiter = '/\n'
  oracle_ddl = open(filepath,'r').read().split(delimiter)
  res = convert(oracle_ddl)
  open(outfile,'w').write(res)