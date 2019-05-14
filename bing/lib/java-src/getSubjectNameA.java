package com.haodou.hive.bing;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.HashMap;
public class getSubjectNameA extends UDF {
    
       public String evaluate(String domain,String url,String cityname,String sdate) {
        
        String rs = "";
        String cleanUrl = url.replaceAll("[?#].*","");
        HashMap<String,String> kv = new HashMap<String,String>();
        kv.put("/recipe" , "菜谱首页");
        kv.put("/recipe/all" , "菜谱大全首页");
        kv.put("/recipe/collect" , "精选集首页");
        kv.put("/recipe/album/" , "菜谱专辑首页"); //  --2013-11-12 16:49:38 新增);
        kv.put("/recipe/expert" , "菜谱达人首页");
        kv.put("/recipe/food" , "食材大全首页");
        kv.put("/recipe/top" , "菜谱排行榜首页");
        kv.put("/topic/mix" , "综合专题首页");
        kv.put("/topic/pai" , "去哪吃专题首页");
        
        if (domain == "shop.haodou.com") {
            if (cleanUrl == "/")
                rs = "商城首页";
            else if (cleanUrl.startsWith("/life.php"))
                rs = "生活馆";
            else if (cleanUrl.startsWith("/lifeall.php"))
                rs = "生活馆列表页";
            else if (cleanUrl.startsWith("/gift.php"))
                rs = "礼品馆";
            else if (cleanUrl.startsWith("/all.php"))
                rs = "商品列表页";
            else if (cleanUrl.startsWith("/view.php"))
                rs = "商品内容页";  //原：商品明细内容页
            else if (cleanUrl.startsWith("/my.php"))
                rs = "我的商城";
            else
                rs = "商城未知主题";
        }
        else if (domain == "group.haodou.com") {
            if (cleanUrl == "/")
                rs = "广场首页";
            else if (cleanUrl.startsWith("/5/"))
                rs = "做美食-乐在厨房";
            else if (cleanUrl.startsWith("/6/"))
                rs = "做美食-营养健康";
            else if (cleanUrl.startsWith("/8/"))
                rs = "做美食-厨房宝典";
            else if (cleanUrl.startsWith("/2/"))
                rs = "去哪吃-美食探店";
            else if (cleanUrl.startsWith("/3/"))
                rs = "去哪吃-特色小吃";
            else if (cleanUrl.startsWith("/24/"))
                rs = "去哪吃-美食爆料";
            else if (cleanUrl.startsWith("/25/"))
                rs = "去哪吃-同城活动";
            else if (cleanUrl.startsWith("/12/"))
                rs = "爱生活-好好生活";
            else if (cleanUrl.startsWith("/11/"))
                rs = "去哪吃-摄影天地";
            else if (cleanUrl.startsWith("/10/"))
                rs = "去哪吃-游山玩水";
            else if (cleanUrl.startsWith("/23/"))
                rs = "去哪吃-亲子乐园";
            else if (cleanUrl.startsWith("/26/"))
                rs = "去哪吃-品牌馆";
            else if (cleanUrl.startsWith("/17/"))
                rs = "站务交流-公告";
            else if (cleanUrl.startsWith("/16/"))
                rs = "站务交流-帮助";
            else if (cleanUrl.startsWith("/topic-")) {
                //TODO: 
            }
            else rs = "广场未知主题";
        }
        else if (domain == "www.qunachi.com") {
            //--去哪吃统计页面.docx
            if (cleanUrl == "/")
                rs = "去哪吃首页";
            else if (cleanUrl.matches("^/share/[0-9]+"))
                rs = "美食详情页";
            else if (cleanUrl.startsWith("/search/share/"))
                rs = "美食搜索页";
            else if (cleanUrl.contains("/share/tag/"))
                rs = "美食标签聚合页";
            else if (cleanUrl.contains("/share/"))
                rs = "美食列表页";
            else if (cleanUrl.contains("/plaza/"))
                rs = "美食列表页";
            else if (cleanUrl.matches("^/rank/[0-9]+"))
                rs = "排行榜详情页";
            else if (cleanUrl.contains("/rank/"))
                rs = "排行榜频道页";
            else if (cleanUrl.matches("^/shop/[0-9]+"))
                rs = "餐馆详情页";
            else if (cleanUrl.startsWith("/search/shop/"))
                rs = "餐馆搜索页";
            else if (cleanUrl.startsWith("/search/subshop/"))
                rs = "分店搜索页";
            else if (cleanUrl.contains("/shop/"))
                rs = "餐馆列表页";
            else if (cleanUrl.contains("/lists/lecheng/"))
                rs = "乐乘专题页";
            else if (cleanUrl.contains("/album/collect/"))
                rs = "专辑征集页";
            else if (cleanUrl.contains("/album/"))
                rs = "专辑页";
            else if (cleanUrl.matches("^/topic/[0-9]+"))
                rs = "探店详情页";
            else if (cleanUrl.startsWith("/topic/create"))
                rs = "发表探店页";
            else if (cleanUrl.contains("/topic/?type=special"))
                rs = "探店频道-特色小吃";
            else if (cleanUrl.contains("/topic/"))
                rs = "探店频道-走街寻店";
            else if (cleanUrl.contains("/expert/"))
                rs = "美食地主页";
            else if (cleanUrl.startsWith("/search/user/"))
                rs = "饭友搜索页";
            else if (cleanUrl.contains("/lists/free/"))
                rs = "免费大餐列表页";
            else if (cleanUrl.startsWith("/subject/"))
                rs = "美食专题页";
            else
                rs = "去哪吃未知主题";
        }
        else if (domain == "wo.qunachi.com")
            rs = "去哪吃豆窝";
        else if (kv.containsKey(cleanUrl))
            rs = kv.get(cleanUrl);
            
        else if (cleanUrl.startsWith("/topic/home"))
            rs = "专题首页";
        else if (cleanUrl.startsWith("/topic/recipe"))
            rs = "菜谱专题首页";
        else if (cleanUrl.startsWith("/topic/")) {
            //TODO:
            /**
                case dw.get_www_topic_type(p_domin, p_cleanUrl, p_statis_date)
                    when '0' then '全站专题内容页'
                    when '1' then '菜谱专题内容页'
                    when '2' then '去哪吃专题内容页'
                    when '3' then '综合专题内容页'
                    when '4' then '推广专题内容页'
                    else '未知专题内容页'
                end
            **/
            
        }
        else if (cleanUrl.startsWith("/pai/collect"))
            rs = "美食攻略首页";
        else if (cleanUrl.matches("^/pai/album/.*\\.html.*"))
            rs = "美食专辑内容页"; //--2013-8-1 17:36:14 新增
        else if (cleanUrl.startsWith("/pai/expert"))
            rs = "美食地主首页";
        else if (cleanUrl.startsWith("/pai/user/")) rs = "我的美食";
        else if (cleanUrl.startsWith("/pai/tag/")) rs = "美食标签页";
        else if (cleanUrl.startsWith("/recipe/album/tag/")) rs = "菜谱专辑标签页";  //--2013-11-12 16:49:38 新增;
        else if (cleanUrl.startsWith("/recipe-")) rs = "菜谱标签页";
        else if (cleanUrl.startsWith("/recipe/all/")) rs = "菜谱标签页";
        else if (cleanUrl.startsWith("/recipe/all?p=")) rs = "菜谱大全首页";
        else if (cleanUrl.startsWith("/recipe/food/qa")) rs = "食材问题库内容页";
        else if (cleanUrl.matches("^/recipe/collect/[0-9]+") ) rs =  "菜谱精选集内容页";
        else if (cleanUrl.matches("^/recipe/album/[0-9]+") ) rs =  "菜谱专辑内容页";  //--2013-11-12 16:49:38 新增;
        else if (cleanUrl.matches("^/recipe/food/[0-9]+") ) rs =  "食材大全内容页";
        else if (cleanUrl.matches("^/recipe/user/[0-9]+") ) rs =  "用户的菜谱页";
        else if (cleanUrl.matches("^/recipe/userphoto/[0-9]+") ) rs =  "用户的菜谱页";  //--2013-5-14 10:03:46 新增规则;
        else if (cleanUrl.matches("^/recipe/[0-9]+") ) rs =  "菜谱内容页";
        else if (cleanUrl.startsWith("/help/center.php"))
            rs = "帮助中心";
        else
            rs = "未分类主题";
        
        return rs;
    }
}