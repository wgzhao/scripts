package com.haodou.hive.bing;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.SimpleDateFormat;
import java.util.*;

public class getUnixTimestamp extends UDF {
    public long evaluate(String pDate, String pPattern) {
        long ut = 0
        try {
            fmt = new java.text.SimpleDateFormat(pPattern)
            ut = fmt.parse(pDate).getTime() / 1000;
        } catch (Exception err) {
            err.printStackTrace();
        }
        return ut;
    }
}

