CREATE TABLE SOLARDATA (
    YEARID INTEGER NOT NULL,
    DATA CHAR(7) NOT NULL,
    DATAINT INTEGER NOT NULL
);

COPY SOLARDATA (YEARID, DATA, DATAINT) FROM stdin;
1900	0x04bd8	19416
1901	0x04ae0	19168
1902	0x0a570	42352
1903	0x054d5	21717
1904	0x0d260	53856
1905	0x0d950	55632
1906	0x16554	91476
1907	0x056a0	22176
1908	0x09ad0	39632
1909	0x055d2	21970
1910	0x04ae0	19168
1911	0x0a5b6	42422
1912	0x0a4d0	42192
1913	0x0d250	53840
1914	0x1d255	119381
1915	0x0b540	46400
1916	0x0d6a0	54944
1917	0x0ada2	44450
1918	0x095b0	38320
1919	0x14977	84343
1920	0x04970	18800
1921	0x0a4b0	42160
1922	0x0b4b5	46261
1923	0x06a50	27216
1924	0x06d40	27968
1925	0x1ab54	109396
1926	0x02b60	11104
1927	0x09570	38256
1928	0x052f2	21234
1929	0x04970	18800
1930	0x06566	25958
1931	0x0d4a0	54432
1932	0x0ea50	59984
1933	0x06e95	28309
1934	0x05ad0	23248
1935	0x02b60	11104
1936	0x186e3	100067
1937	0x092e0	37600
1938	0x1c8d7	116951
1939	0x0c950	51536
1940	0x0d4a0	54432
1941	0x1d8a6	120998
1942	0x0b550	46416
1943	0x056a0	22176
1944	0x1a5b4	107956
1945	0x025d0	9680
1946	0x092d0	37584
1947	0x0d2b2	53938
1948	0x0a950	43344
1949	0x0b557	46423
1950	0x06ca0	27808
1951	0x0b550	46416
1952	0x15355	86869
1953	0x04da0	19872
1954	0x0a5d0	42448
1955	0x14573	83315
1956	0x052d0	21200
1957	0x0a9a8	43432
1958	0x0e950	59728
1959	0x06aa0	27296
1960	0x0aea6	44710
1961	0x0ab50	43856
1962	0x04b60	19296
1963	0x0aae4	43748
1964	0x0a570	42352
1965	0x05260	21088
1966	0x0f263	62051
1967	0x0d950	55632
1968	0x05b57	23383
1969	0x056a0	22176
1970	0x096d0	38608
1971	0x04dd5	19925
1972	0x04ad0	19152
1973	0x0a4d0	42192
1974	0x0d4d4	54484
1975	0x0d250	53840
1976	0x0d558	54616
1977	0x0b540	46400
1978	0x0b5a0	46496
1979	0x195a6	103846
1980	0x095b0	38320
1981	0x049b0	18864
1982	0x0a974	43380
1983	0x0a4b0	42160
1984	0x0b27a	45690
1985	0x06a50	27216
1986	0x06d40	27968
1987	0x0af46	44870
1988	0x0ab60	43872
1989	0x09570	38256
1990	0x04af5	19189
1991	0x04970	18800
1992	0x064b0	25776
1993	0x074a3	29859
1994	0x0ea50	59984
1995	0x06b58	27480
1996	0x055c0	21952
1997	0x0ab60	43872
1998	0x096d5	38613
1999	0x092e0	37600
2000	0x0c960	51552
2001	0x0d954	55636
2002	0x0d4a0	54432
2003	0x0da50	55888
2004	0x07552	30034
2005	0x056a0	22176
2006	0x0abb7	43959
2007	0x025d0	9680
2008	0x092d0	37584
2009	0x0cab5	51893
2010	0x0a950	43344
2011	0x0b4a0	46240
2012	0x0baa4	47780
2013	0x0ad50	44368
2014	0x055d9	21977
2015	0x04ba0	19360
2016	0x0a5b0	42416
2017	0x15176	86390
2018	0x052b0	21168
2019	0x0a930	43312
2020	0x07954	31060
2021	0x06aa0	27296
2022	0x0ad50	44368
2023	0x05b52	23378
2024	0x04b60	19296
2025	0x0a6e6	42726
2026	0x0a4e0	42208
2027	0x0d260	53856
2028	0x0ea65	60005
2029	0x0d530	54576
2030	0x05aa0	23200
2031	0x076a3	30371
2032	0x096d0	38608
2033	0x04bd7	19415
2034	0x04ad0	19152
2035	0x0a4d0	42192
2036	0x1d0b6	118966
2037	0x0d250	53840
2038	0x0d520	54560
2039	0x0dd45	56645
2040	0x0b5a0	46496
2041	0x056d0	22224
2042	0x055b2	21938
2043	0x049b0	18864
2044	0x0a577	42359
2045	0x0a4b0	42160
2046	0x0aa50	43600
2047	0x1b255	111189
2048	0x06d20	27936
2049	0x0ada0	44448
\.


CREATE UNIQUE INDEX IDX_SOLARDATA_YEARID ON SOLARDATA USING BTREE (YEARID);


create or replace FUNCTION f_GetLunar(i_SolarDay DATE) RETURNS VARCHAR
AS $$
DECLARE
v_OffSet INT;
v_Lunar INT; -- 农历年是否含闰月,几月是闰月,闰月天数,其它月天数
v_YearDays INT; -- 农历年所含天数
v_MonthDays INT; -- 农历月所含天数
v_LeapMonthDays INT; -- 农历闰月所含天数
v_LeapMonth INT; -- 农历年闰哪个月 1-12 , 没闰传回 0
v_LeapFlag INT; -- 某农历月是否为闰月 1:是 0:不是
v_MonthNo INT; -- 某农历月所对应的2进制数 如农历3月: 001000000000 
i INT;
j INT; 
k INT;

v_Year INT; -- i_SolarDay 对应的农历年
v_Month INT; -- i_SolarDay 对应的农历月
v_Day INT; -- i_SolarDay 对应的农历日

o_OutputDate VARCHAR(255); -- 返回值 格式：农历 ****年 **(闰)月 **日

e_ErrMsg VARCHAR(200);

BEGIN


--输入参数过滤
--select cast(i_SolarDay::timestamp as date) into i_SolarDay;
--IF i_SolarDay<TO_DATE('1900-01-31','YYYY-MM-DD') OR i_SolarDay>=TO_DATE('2050-01-23','YYYY-MM-DD') THEN
--RAISE EXCEPTION 'please input date between 1900/1/31 and 2050/1/23';
--END IF ;

-- i_SolarDay 到 1900-01-30(即农历1900-01-01的前一天) 的天数
--v_OffSet := datediff('day',cast('1900/1/30' as date), cast(i_SolarDay as date));
select i_SolarDay::date - '1900/1/30'::date into v_OffSet;
-- 确定农历年开始
i := 1900;
WHILE i < 2050 AND v_OffSet > 0 LOOP
v_YearDays := 348; -- 29*12 以每年12个农历月,每个农历月含29个农历日为基数
v_LeapMonthDays := 0;

-- 取出农历年是否含闰月,几月是闰月,闰月天数,其它月天数
-- 如农历2001年: 0x0d954(16进制) -> 55636(10进制) -> 0 110110010101 0100(2进制)
-- 1,2,4,5,8,10,12月大, 3,6,7,9,11月小, 4月为闰月，闰月小
SELECT DataInt INTO v_Lunar FROM SolarData WHERE YearId = i;


-- 传回农历年的总天数
j := 32768; -- 100000000000 0000 -> 32768
-- 0 110110010101 0100 -> 55636(农历2001年)
-- 依次判断v_Lunar年个月是否为大月，是则加一天 
WHILE j > 8 LOOP -- 闰月另行判断 8 -> 0 000000000000 1000 
IF bitand(v_Lunar::bit(32), j::bit(32))::int + 0 > 0 then
v_YearDays := v_YearDays + 1;
END IF;
j := j/2; -- 判断下一个月是否为大
END LOOP;


-- 传回农历年闰哪个月 1-12 , 没闰传回 0 15 -> 1 0000
v_LeapMonth := bitand(v_Lunar::bit(32), 15::bit(32))::int + 0;

-- 传回农历年闰月的天数 ,加在年的总天数上
IF v_LeapMonth > 0 THEN
-- 判断闰月大小 65536 -> 1 000000000000 0000 
IF BITAND(v_Lunar::bit(32), 65536::bit(32))::int + 0 > 0 THEN
v_LeapMonthDays := 30;
ELSE
v_LeapMonthDays := 29;
END IF;
v_YearDays := v_YearDays + v_LeapMonthDays;
END IF;

v_OffSet := v_OffSet - v_YearDays;
i := i + 1;
END LOOP;

IF v_OffSet <= 0 THEN
-- i_SolarDay 在所属农历年(即i年)中的第 v_OffSet 天 
v_OffSet := v_OffSet + v_YearDays; 
i := i - 1;
END IF;
-- 确定农历年结束
v_Year := i;

-- 确定农历月开始
i := 1;
SELECT DataInt INTO v_Lunar FROM SolarData WHERE YearId = v_Year;

-- 判断那个月是润月
-- 如农历2001年 (55636,15 -> 0 1101100101010100, 1111 -> 4) 即润4月,且闰月小
v_LeapMonth := BITAND(v_Lunar::bit(32), 15::bit(32))::int + 0; 
v_LeapFlag := 0;

WHILE i < 13 AND v_OffSet > 0 LOOP
-- 判断是否为闰月
v_MonthDays := 0;
IF (v_LeapMonth > 0 AND i = (v_LeapMonth + 1) AND v_LeapFlag = 0) THEN
-- 是闰月
i := i - 1;
k := i; -- 保存是闰月的时i的值
v_LeapFlag := 1;
-- 传回农历年闰月的天数
IF BITAND(v_Lunar::bit(32), 65536::bit(32))::date + 0 > 0 THEN
v_MonthDays := 30;
ELSE
v_MonthDays := 29;
END IF;

ELSE
-- 不是闰月
j := 1;
v_MonthNo := 65536;
-- 计算 i 月对应的2进制数 如农历3月: 001000000000
WHILE j<= i LOOP
v_MonthNo := v_MonthNo/2;
j := j + 1;
END LOOP;
-- 计算农历 v_Year 年 i 月的天数
IF BITAND(v_Lunar::bit(32), v_MonthNo::bit(32))::int + 0 > 0 THEN
v_MonthDays := 30;
ELSE
v_MonthDays := 29;
END IF;
END IF;

-- 解除闰月
IF v_LeapFlag = 1 AND i = v_LeapMonth +1 THEN
v_LeapFlag := 0;
END IF;
v_OffSet := v_OffSet - v_MonthDays;
i := i + 1;
END LOOP;

IF v_OffSet <= 0 THEN
-- i_SolarDay 在所属农历月(即i月)中的第 v_OffSet 天 
v_OffSet := v_OffSet + v_MonthDays;
i := i - 1;
END IF;

-- 确定农历月结束
v_Month := i;

-- 确定农历日结束
v_Day := v_OffSet;

-- 格式化返回值
o_OutputDate := '农历 '||TO_CHAR(v_Year,'9999')||'年 ';

IF k = i THEN
o_OutputDate := o_OutputDate || TO_CHAR(v_Month,'99') || '(润)月 ';
ELSE
o_OutputDate := o_OutputDate || TO_CHAR(v_Month,'99') || '月 ';
END IF;
o_OutputDate := o_OutputDate || TO_CHAR(v_Day,'99') || '日';

RETURN o_OutputDate;

EXCEPTION
WHEN OTHERS THEN
e_ErrMsg :=SUBSTR(SQLERRM,1,200);
RETURN e_ErrMsg;
END;
$$ LANGUAGE plpgsql;

/

