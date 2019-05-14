#! /usr/bin/env python
# -*- coding: utf-8 -*-

#基础数据由常正每月月底提供

import sys, os
import datetime, time
import logging
import random
import MySQLdb

def isdate(s):
  try:
    time.strptime(str(s).replace('-',''),"%Y%m%d")
    return True
  except:
    return False

def mklogfile(s):
  if not os.path.exists(s):
    f=open(s,'w')
    f.write('.log\n')
    f.close()
    os.chmod(s, 0666)

# 目标型
def evaluate_target(prevalue, targetvalue, day, days, weekday, unit, index):
  random.seed(ts_statisdate*indexfactor[index])
  if targetvalue==0:
    rt = 0
  elif day==days:
    rt = targetvalue*unit + weekfactor1[weekday]*0.3459*unit*random.random()
    if rt==targetvalue*unit:
      rt = targetvalue*unit + 0.3459*unit*random.random()
  else:
    rt = prevalue*unit + weekfactor0[weekday]*(1.0*day/days)*(targetvalue-prevalue)*unit + weekfactor1[weekday]*0.0123*targetvalue*unit*random.random()
  if rt<0:
    rt = 0
  return rt

# 波动型
def evaluate_wave(targetvalue, day, days, weekday, index):
  random.seed(ts_statisdate*indexfactor[index])
  if day==days:
    rt = targetvalue
  else:
    rt = targetvalue + weekfactor1[weekday]*random.random()*0.0321*targetvalue
  return rt

# 单调型
def evaluate_monotone(prevalue, targetvalue, day, days, unit, index):
  random.seed(ts_statisdate*indexfactor[index])
  if targetvalue==0:
    rt = 0
  elif day==days:
    rt = targetvalue*unit + (random.randint(0,2)-1)*0.3459*unit*random.random()
    if rt==targetvalue*unit:
      rt = targetvalue*unit + 0.3459*unit*random.random()
  else:
    rt = prevalue + random.randint(0, int((2.345-1.0*day/days)*(targetvalue*unit-prevalue)/(days-day)))
  return rt

# MySQL取值
def getwebregvalue(pdate):
  sqlcursor.execute("select web_user from showdb.rpt_user_index where statis_date='%(statis_date)s'" % {'statis_date':pdate})
  rt = sqlcursor.fetchone()[0]
  sqlcursor.fetchall()
  return rt

def getappregvalue(pdate):
  sqlcursor.execute("select app_user from showdb.rpt_user_index where statis_date='%(statis_date)s'" % {'statis_date':pdate})
  rt = sqlcursor.fetchone()[0]
  sqlcursor.fetchall()
  return rt

reload(sys)
sys.setdefaultencoding('utf-8')

pid = os.getpid()
rundate = datetime.date.today().strftime("%Y%m%d")
rundir = os.path.dirname(os.path.abspath(__file__))
runfilename = os.path.splitext(os.path.split(os.path.abspath(__file__))[1])[0]
homedir= rundir + "/.."
logdir = homedir + "/log"
tmpdir =  homedir + "/tmp"
if not os.path.exists(logdir):
  os.mkdir(logdir,0777)
if not os.path.exists(tmpdir):
  os.mkdir(tmpdir,0777)
logfile = '%(dir)s%(sep)s%(filename)s.log' % {'dir':logdir,'sep':os.sep,'filename':runfilename,'rundate':rundate,'pid':pid}
if not os.path.exists(logfile):
  mklogfile(logfile)

logger = logging.getLogger("showdb")
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(logfile)
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(levelname)s - %(message)s"))
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logging.Formatter("%(asctime)s\tpid#%(process)d\t%(filename)s(%(lineno)d)\n%(message)s"))
logger.addHandler(consoleHandler)

logger.info("begin execute... %s" % str(sys.argv))

if len(sys.argv)>1:
  statisdate = sys.argv[1].replace('-','')
else:
  statisdate = (datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y%m%d")

if isdate(statisdate)==False:
  logger.error("unconverted date %s" % statisdate)
  sys.exit(-101)

dt_statisdate = datetime.datetime.strptime(statisdate,"%Y%m%d")
ts_statisdate = time.mktime(time.strptime(statisdate,'%Y%m%d'))
statis_date = dt_statisdate.strftime("%Y-%m-%d")
statis_weekday = dt_statisdate.isoweekday()
pre_date = (dt_statisdate + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

uv = 'uv'
pv = 'pv'
dur = 'dur'
webreg = 'webreg'
appreg = 'appreg'
azmonthact = 'azmonthact'
ipmonthact = 'ipmonthact'
appdayact = 'appdayact'
appdur = 'appdur'

indexfactor={'uv':13,'pv':17,'dur':19,'webreg':23,'appreg':29,'azmonthact':31,'ipmonthact':37,'appdayact':43,'appdur':47}
weekfactor0=[1,1.0712,1.034,0.9256,0.9978,0.9390,1.0512,1.0734]
weekfactor1=[1,-1,1,-1,1,-1,1,1]

baseline={
'2011-01-31':{'uv':0.30   ,'pv':0.70   ,'dur':70  ,'webreg':0.10   ,'appreg':0.00   ,'azmonthact':0.00   ,'ipmonthact':0.00   ,'appdayact':0.00   ,'appdur':0  },
'2011-02-28':{'uv':1.00   ,'pv':2.00   ,'dur':90  ,'webreg':0.50   ,'appreg':0.00   ,'azmonthact':0.00   ,'ipmonthact':0.00   ,'appdayact':0.00   ,'appdur':0  },
'2011-03-31':{'uv':1.50   ,'pv':3.40   ,'dur':90  ,'webreg':2.00   ,'appreg':0.00   ,'azmonthact':0.00   ,'ipmonthact':0.00   ,'appdayact':0.00   ,'appdur':0  },
'2011-04-30':{'uv':1.50   ,'pv':3.50   ,'dur':95  ,'webreg':4.00   ,'appreg':0.00   ,'azmonthact':0.00   ,'ipmonthact':0.00   ,'appdayact':0.00   ,'appdur':0  },
'2011-05-31':{'uv':2.00   ,'pv':4.00   ,'dur':98  ,'webreg':6.00   ,'appreg':0.00   ,'azmonthact':0.00   ,'ipmonthact':0.00   ,'appdayact':0.00   ,'appdur':0  },
'2011-06-30':{'uv':2.50   ,'pv':5.20   ,'dur':100 ,'webreg':8.00   ,'appreg':0.10   ,'azmonthact':0.10   ,'ipmonthact':0.00   ,'appdayact':0.10   ,'appdur':50 },
'2011-07-31':{'uv':3.00   ,'pv':7.00   ,'dur':110 ,'webreg':11.00  ,'appreg':0.50   ,'azmonthact':2.00   ,'ipmonthact':0.00   ,'appdayact':1.00   ,'appdur':60 },
'2011-08-31':{'uv':3.60   ,'pv':7.60   ,'dur':115 ,'webreg':14.00  ,'appreg':2.00   ,'azmonthact':4.00   ,'ipmonthact':0.00   ,'appdayact':2.00   ,'appdur':60 },
'2011-09-30':{'uv':4.00   ,'pv':9.00   ,'dur':125 ,'webreg':16.00  ,'appreg':3.50   ,'azmonthact':6.00   ,'ipmonthact':1.00   ,'appdayact':3.50   ,'appdur':63 },
'2011-10-31':{'uv':5.00   ,'pv':11.00  ,'dur':140 ,'webreg':18.00  ,'appreg':5.00   ,'azmonthact':8.00   ,'ipmonthact':3.00   ,'appdayact':4.00   ,'appdur':65 },
'2011-11-30':{'uv':5.50   ,'pv':13.00  ,'dur':150 ,'webreg':20.00  ,'appreg':6.50   ,'azmonthact':10.00  ,'ipmonthact':5.00   ,'appdayact':5.50   ,'appdur':68 },
'2011-12-31':{'uv':7.00   ,'pv':17.00  ,'dur':160 ,'webreg':22.00  ,'appreg':8.00   ,'azmonthact':12.00  ,'ipmonthact':10.00  ,'appdayact':6.00   ,'appdur':70 },
'2012-01-31':{'uv':9.00   ,'pv':20.00  ,'dur':160 ,'webreg':24.00  ,'appreg':15.00  ,'azmonthact':16.00  ,'ipmonthact':13.00  ,'appdayact':7.00   ,'appdur':78 },
'2012-02-29':{'uv':7.00   ,'pv':22.00  ,'dur':180 ,'webreg':25.00  ,'appreg':25.00  ,'azmonthact':20.00  ,'ipmonthact':15.00  ,'appdayact':7.50   ,'appdur':82 },
'2012-03-31':{'uv':10.00  ,'pv':26.00  ,'dur':175 ,'webreg':26.00  ,'appreg':38.00  ,'azmonthact':24.00  ,'ipmonthact':16.00  ,'appdayact':9.00   ,'appdur':90 },
'2012-04-30':{'uv':14.00  ,'pv':33.00  ,'dur':180 ,'webreg':27.00  ,'appreg':50.00  ,'azmonthact':28.00  ,'ipmonthact':17.00  ,'appdayact':10.00  ,'appdur':94 },
'2012-05-31':{'uv':20.00  ,'pv':42.00  ,'dur':180 ,'webreg':28.00  ,'appreg':60.00  ,'azmonthact':32.00  ,'ipmonthact':18.00  ,'appdayact':12.00  ,'appdur':100},
'2012-06-30':{'uv':25.00  ,'pv':56.00  ,'dur':190 ,'webreg':30.00  ,'appreg':70.00  ,'azmonthact':36.00  ,'ipmonthact':20.00  ,'appdayact':12.50  ,'appdur':108},
'2012-07-31':{'uv':31.00  ,'pv':70.00  ,'dur':195 ,'webreg':35.00  ,'appreg':80.00  ,'azmonthact':42.00  ,'ipmonthact':22.00  ,'appdayact':13.00  ,'appdur':113},
'2012-08-31':{'uv':37.00  ,'pv':81.00  ,'dur':197 ,'webreg':43.00  ,'appreg':95.00  ,'azmonthact':50.00  ,'ipmonthact':24.00  ,'appdayact':15.00  ,'appdur':120},
'2012-09-30':{'uv':41.00  ,'pv':85.00  ,'dur':200 ,'webreg':50.00  ,'appreg':110.00 ,'azmonthact':58.00  ,'ipmonthact':26.00  ,'appdayact':18.00  ,'appdur':122},
'2012-10-31':{'uv':45.00  ,'pv':90.00  ,'dur':205 ,'webreg':57.00  ,'appreg':130.00 ,'azmonthact':68.00  ,'ipmonthact':28.00  ,'appdayact':22.00  ,'appdur':127},
'2012-11-30':{'uv':48.00  ,'pv':96.00  ,'dur':208 ,'webreg':65.00  ,'appreg':145.00 ,'azmonthact':77.00  ,'ipmonthact':30.00  ,'appdayact':26.00  ,'appdur':130},
'2012-12-31':{'uv':50.00  ,'pv':100.00 ,'dur':210 ,'webreg':70.00  ,'appreg':160.00 ,'azmonthact':87.00  ,'ipmonthact':33.00  ,'appdayact':30.00  ,'appdur':135},
'2013-01-31':{'uv':51.00  ,'pv':105.00 ,'dur':212 ,'webreg':72.00  ,'appreg':170.00 ,'azmonthact':100.00 ,'ipmonthact':36.00  ,'appdayact':31.00  ,'appdur':140},
'2013-02-28':{'uv':55.00  ,'pv':120.00 ,'dur':215 ,'webreg':75.00  ,'appreg':185.00 ,'azmonthact':110.00 ,'ipmonthact':39.00  ,'appdayact':33.00  ,'appdur':142},
'2013-03-31':{'uv':60.00  ,'pv':135.00 ,'dur':216 ,'webreg':79.00  ,'appreg':195.00 ,'azmonthact':120.00 ,'ipmonthact':42.00  ,'appdayact':35.00  ,'appdur':150},
'2013-04-30':{'uv':66.00  ,'pv':148.00 ,'dur':218 ,'webreg':82.00  ,'appreg':208.00 ,'azmonthact':130.00 ,'ipmonthact':45.00  ,'appdayact':37.00  ,'appdur':156},
'2013-05-31':{'uv':70.00  ,'pv':152.00 ,'dur':220 ,'webreg':86.00  ,'appreg':218.00 ,'azmonthact':140.00 ,'ipmonthact':47.00  ,'appdayact':38.00  ,'appdur':160},
'2013-06-30':{'uv':80.00  ,'pv':160.00 ,'dur':220 ,'webreg':90.00  ,'appreg':230.00 ,'azmonthact':150.00 ,'ipmonthact':50.00  ,'appdayact':40.00  ,'appdur':164},
'2013-07-31':{'uv':88.00  ,'pv':172.00 ,'dur':222 ,'webreg':94.00  ,'appreg':240.00 ,'azmonthact':165.00 ,'ipmonthact':53.00  ,'appdayact':42.00  ,'appdur':170},
'2013-08-31':{'uv':90.00  ,'pv':174.00 ,'dur':225 ,'webreg':98.00  ,'appreg':255.00 ,'azmonthact':180.00 ,'ipmonthact':58.00  ,'appdayact':45.00  ,'appdur':176},
'2013-09-30':{'uv':95.00  ,'pv':180.00 ,'dur':232 ,'webreg':102.00 ,'appreg':270.00 ,'azmonthact':200.00 ,'ipmonthact':61.00  ,'appdayact':47.00  ,'appdur':180},
'2013-10-31':{'uv':97.00  ,'pv':183.00 ,'dur':237 ,'webreg':106.00 ,'appreg':295.00 ,'azmonthact':220.00 ,'ipmonthact':64.00  ,'appdayact':48.00  ,'appdur':182},
'2013-11-30':{'uv':98.00  ,'pv':190.00 ,'dur':235 ,'webreg':110.00 ,'appreg':310.00 ,'azmonthact':240.00 ,'ipmonthact':67.00  ,'appdayact':49.00  ,'appdur':190},
'2013-12-31':{'uv':97.00  ,'pv':184.00 ,'dur':237 ,'webreg':115.00 ,'appreg':330.00 ,'azmonthact':260.00 ,'ipmonthact':70.00  ,'appdayact':50.00  ,'appdur':195},
'2014-01-31':{'uv':100.00 ,'pv':197.00 ,'dur':240 ,'webreg':125.00 ,'appreg':340.00 ,'azmonthact':280.00 ,'ipmonthact':73.00  ,'appdayact':55.00  ,'appdur':200},
'2014-02-28':{'uv':106.00 ,'pv':210.00 ,'dur':237 ,'webreg':136.00 ,'appreg':360.00 ,'azmonthact':300.00 ,'ipmonthact':78.00  ,'appdayact':60.00  ,'appdur':210},
'2014-03-31':{'uv':110.00 ,'pv':230.00 ,'dur':235 ,'webreg':145.00 ,'appreg':380.00 ,'azmonthact':320.00 ,'ipmonthact':84.00  ,'appdayact':63.00  ,'appdur':212},
'2014-04-30':{'uv':112.00 ,'pv':235.00 ,'dur':233 ,'webreg':160.00 ,'appreg':400.00 ,'azmonthact':340.00 ,'ipmonthact':90.00  ,'appdayact':67.00  ,'appdur':218},
'2014-05-31':{'uv':120.00 ,'pv':243.00 ,'dur':230 ,'webreg':180.00 ,'appreg':420.00 ,'azmonthact':370.00 ,'ipmonthact':96.00  ,'appdayact':70.00  ,'appdur':223},
'2014-06-30':{'uv':110.00 ,'pv':230.00 ,'dur':240 ,'webreg':190.00 ,'appreg':460.00 ,'azmonthact':400.00 ,'ipmonthact':105.00 ,'appdayact':80.00  ,'appdur':230},
'2014-07-31':{'uv':100.00 ,'pv':200.00 ,'dur':245 ,'webreg':200.00 ,'appreg':500.00 ,'azmonthact':430.00 ,'ipmonthact':113.00 ,'appdayact':90.00  ,'appdur':260},
'2014-08-31':{'uv':105.00 ,'pv':218.00 ,'dur':248 ,'webreg':210.00 ,'appreg':540.00 ,'azmonthact':460.00 ,'ipmonthact':121.00 ,'appdayact':100.00 ,'appdur':265},
'2014-09-30':{'uv':107.00 ,'pv':221.00 ,'dur':250 ,'webreg':220.00 ,'appreg':590.00 ,'azmonthact':490.00 ,'ipmonthact':130.00 ,'appdayact':110.00 ,'appdur':270},
'2014-10-31':{'uv':111.00 ,'pv':234.00 ,'dur':253 ,'webreg':235.83 ,'appreg':596.67 ,'azmonthact':513.57 ,'ipmonthact':136.57 ,'appdayact':115.29 ,'appdur':272},
'2014-11-30':{'uv':114.50 ,'pv':244.00 ,'dur':255 ,'webreg':248.20 ,'appreg':627.33 ,'azmonthact':544.86 ,'ipmonthact':145.16 ,'appdayact':123.39 ,'appdur':275},
'2014-12-31':{'uv':114.80 ,'pv':245.10 ,'dur':254 ,'webreg':257.36 ,'appreg':650.89 ,'azmonthact':552.44 ,'ipmonthact':148.95 ,'appdayact':127.32 ,'appdur':274},
'2015-01-31':{'uv':114.90 ,'pv':246.10 ,'dur':255 ,'webreg':263.06 ,'appreg':668.11 ,'azmonthact':550.21 ,'ipmonthact':150.45 ,'appdayact':128.74 ,'appdur':276},
'2015-02-18':{'uv':121.30 ,'pv':288.40 ,'dur':272 ,'webreg':279.68 ,'appreg':684.20 ,'azmonthact':581.10 ,'ipmonthact':172.43 ,'appdayact':134.50 ,'appdur':284},
'2015-02-19':{'uv':79.43  ,'pv':161.50 ,'dur':241 ,'webreg':280.20 ,'appreg':685.10 ,'azmonthact':579.50 ,'ipmonthact':170.70 ,'appdayact':87.80  ,'appdur':271},
'2015-02-28':{'uv':84.58  ,'pv':172.60 ,'dur':248 ,'webreg':288.52 ,'appreg':691.10 ,'azmonthact':531.32 ,'ipmonthact':142.20 ,'appdayact':104.50 ,'appdur':272},
'2015-03-31':{'uv':97.23  ,'pv':199.47 ,'dur':253 ,'webreg':315.02 ,'appreg':721.81 ,'azmonthact':553.47 ,'ipmonthact':149.87 ,'appdayact':116.32 ,'appdur':276},
'2015-04-30':{'uv':108.93 ,'pv':238.81 ,'dur':252 ,'webreg':342.73 ,'appreg':755.36 ,'azmonthact':591.83 ,'ipmonthact':155.47 ,'appdayact':119.06 ,'appdur':277},
'2015-05-31':{'uv':115.54 ,'pv':277.10 ,'dur':264 ,'webreg':370.69 ,'appreg':792.54 ,'azmonthact':633.23 ,'ipmonthact':177.49 ,'appdayact':124.25 ,'appdur':281},
'2015-06-30':{'uv':121.74 ,'pv':292.36 ,'dur':266 ,'webreg':388.41 ,'appreg':818.72 ,'azmonthact':664.65 ,'ipmonthact':197.36 ,'appdayact':129.13 ,'appdur':285},
'2015-07-31':{'uv':122.91 ,'pv':297.24 ,'dur':265 ,'webreg':391.17 ,'appreg':849.48 ,'azmonthact':695.76 ,'ipmonthact':216.31 ,'appdayact':134.41 ,'appdur':288},
'2015-08-31':{'uv':123.67 ,'pv':298.72 ,'dur':266 ,'webreg':413.84 ,'appreg':895.65 ,'azmonthact':716.94 ,'ipmonthact':275.12 ,'appdayact':138.66 ,'appdur':289},
'2015-09-30':{'uv':124.32 ,'pv':318.93 ,'dur':267 ,'webreg':447.35 ,'appreg':967.23 ,'azmonthact':747.11 ,'ipmonthact':343.53 ,'appdayact':139.74 ,'appdur':289},
'2015-10-31':{'uv':128.85 ,'pv':347.68 ,'dur':268 ,'webreg':466.26 ,'appreg':993.79 ,'azmonthact':772.57 ,'ipmonthact':378.64 ,'appdayact':144.17 ,'appdur':292},
'2015-11-30':{'uv':132.18 ,'pv':361.45 ,'dur':269 ,'webreg':488.58 ,'appreg':1033.87 ,'azmonthact':792.32 ,'ipmonthact':397.75 ,'appdayact':146.89 ,'appdur':295},
'2015-12-31':{'uv':135.36 ,'pv':373.57 ,'dur':269 ,'webreg':495.92 ,'appreg':1055.98 ,'azmonthact':818.40 ,'ipmonthact':437.28 ,'appdayact':148.38 ,'appdur':297}
}

pre_cycle = '2000-01-01'
statis_cycle = '2222-12-31'
for (k, v) in baseline.items():
  if k>pre_cycle and k<statis_date:
    pre_cycle = k
  if k<statis_cycle and k>=statis_date:
    statis_cycle = k

print pre_cycle
print statis_cycle

statis_pos = (datetime.datetime.strptime(statis_date,"%Y-%m-%d") - datetime.datetime.strptime(pre_cycle,"%Y-%m-%d")).days
cycle_length = (datetime.datetime.strptime(statis_cycle,"%Y-%m-%d") - datetime.datetime.strptime(pre_cycle,"%Y-%m-%d")).days

print statis_pos
print cycle_length

dbconn = MySQLdb.connect(host='10.0.11.50',user='webuser',passwd='onlyhaodou321',port=3306,db='showdb',charset='utf8')
sqlcursor = dbconn.cursor()
sqlcursor.execute("set names 'utf8'")

cur_uv = evaluate_target(baseline[pre_cycle][uv], baseline[statis_cycle][uv], statis_pos, cycle_length, statis_weekday, 10000, uv)
cur_pv = evaluate_target(baseline[pre_cycle][pv], baseline[statis_cycle][pv], statis_pos, cycle_length, statis_weekday, 10000, pv)
cur_dur = evaluate_wave(baseline[statis_cycle][dur], statis_pos, cycle_length, statis_weekday, dur)

per_webreg = getwebregvalue(pre_date)
per_appreg = getappregvalue(pre_date)

cur_webreg = evaluate_monotone(per_webreg, baseline[statis_cycle][webreg], statis_pos, cycle_length, 10000, webreg)
cur_appreg = evaluate_monotone(per_appreg, baseline[statis_cycle][appreg], statis_pos, cycle_length, 10000, appreg)
cur_totalreg = cur_webreg + cur_appreg

cur_azmonthact = evaluate_target(baseline[pre_cycle][azmonthact], baseline[statis_cycle][azmonthact], statis_pos, cycle_length, statis_weekday, 10000, azmonthact)
cur_ipmonthact = evaluate_target(baseline[pre_cycle][ipmonthact], baseline[statis_cycle][ipmonthact], statis_pos, cycle_length, statis_weekday, 10000, ipmonthact)

cur_appdayact = evaluate_target(baseline[pre_cycle][appdayact], baseline[statis_cycle][appdayact], statis_pos, cycle_length, statis_weekday, 10000, appdayact)

cur_appdur = evaluate_wave(baseline[statis_cycle][appdur], statis_pos, cycle_length, statis_weekday, appdur)

sql_stmt = R"""
delete from showdb.rpt_web_index where statis_date = '%(statis_date)s';
delete from showdb.rpt_user_index where statis_date = '%(statis_date)s';
delete from showdb.rpt_monthact_index where statis_date = '%(statis_date)s';
delete from showdb.rpt_dayact_index where statis_date = '%(statis_date)s';
delete from showdb.rpt_appdur_index where statis_date = '%(statis_date)s';

insert into showdb.rpt_web_index values ('%(statis_date)s', 'www.haodou.com', %(cur_uv)d, %(cur_pv)d, %(cur_dur)d);
insert into showdb.rpt_user_index values ('%(statis_date)s', %(cur_webreg)d, %(cur_appreg)d, %(cur_totalreg)d);
insert into showdb.rpt_monthact_index values ('%(statis_date)s', %(cur_azmonthact)d, %(cur_ipmonthact)d);
insert into showdb.rpt_dayact_index values ('%(statis_date)s', %(cur_appdayact)d);
insert into showdb.rpt_appdur_index values ('%(statis_date)s', %(cur_appdur)d);
"""

sql = sql_stmt % {'statis_date':statis_date, 
'cur_uv':cur_uv, 'cur_pv':cur_pv, 'cur_dur':cur_dur,
'cur_webreg':cur_webreg, 'cur_appreg':cur_appreg, 'cur_totalreg':cur_totalreg,
'cur_azmonthact':cur_azmonthact, 'cur_ipmonthact':cur_ipmonthact,
'cur_appdayact':cur_appdayact,
'cur_appdur':cur_appdur
}
print sql
sqlcursor.execute(sql)

sqlcursor.close()
dbconn.commit()
dbconn.close()

logger.info("execute end.")
