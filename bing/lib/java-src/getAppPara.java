package com.haodou.hive.bing;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.*;

public class getAppPara extends UDF {
    
    public int evaluate(String para, String key) {
        int pos = para.indexOf("\"" + key + "\"");
        if(pos <= 0) return null;
        String substr = para.substring(pos);
        String[] subs = substr.split(";");
        if(subs.length < 2) return null;
        return subs[1].split(":")[2];
    }
}
