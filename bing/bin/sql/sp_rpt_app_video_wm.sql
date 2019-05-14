-- 视频菜谱周统计表

insert overwrite table bing.rpt_app_video_wm partition(statis_week='${statisweek_firstday}')
    select '*',sum(view_num),avg(dev_num),sum(comment_num),sum(fav_num),sum(collect_num),sum(shared_num),avg(add_ratio)
    from bing.rpt_app_video_dm 
    where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}';
