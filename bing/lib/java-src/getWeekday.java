package com.haodou.hive.bing;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.SimpleDateFormat;
import java.util.*;

public class getWeekday extends UDF {
    
    public int evaluate(String pDate) {
        fmt = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(fmt.parse(pDate));
        } catch (Exception err) {
            err.printStackTrace();
        }
        int wd = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (wd == 0) {
            wd = 7;
        } 
        return wd;
    }
}