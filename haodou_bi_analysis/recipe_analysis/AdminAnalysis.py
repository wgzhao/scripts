#!/usr/bin/python
# -*- coding: utf8 -*-
#coding = utf-8

from datetime import datetime,date
import MySQLdb
import sys


def adminAnalysis(rundate):
        bjmysql = MySQLdb.connect(host='10.0.10.85', user='bi',passwd='bi_haodou!@#',db='dw',charset="utf8")
        bjcur = bjmysql.cursor()
        tjmysql = MySQLdb.connect(host='10.0.11.70',user='bi',passwd='bi_haodou',charset='utf8')
        tjcur = tjmysql.cursor()

        #make it into tuple
        tdate=(rundate,)

        #删除菜谱管理员当天的操作数据，如果存在的话
        # model
        # 1 点赞
        # 2 发私信
        # 3 评论

        bjcur.execute("delete from bi_recipe_admin_operate_summary WHERE createtime='%s' and model in(1,2,3)" % rundate)

        #删除菜谱校验总结当天数据，如果存在的话
        # type
        # 1 菜谱审核数/编辑数/删除数/屏蔽数
        # 2 专辑审核数/隐藏数/删除数
        # 3 作品通过数/删除数

        bjcur.execute("delete from bi_recipe_admin_verify_summary WHERE createtime='%s' and `type` in (1,2,3)" % rundate )

        # 查询需要的数据
        qsqls=["""select au.adminuserid,u.username,count(d.id) from haodou_digg.Digg d
                  inner join haodou_admin.AdminUser au on au.adminuserid=d.userid
                  left outer join haodou_passport.User u on u.userid=au.adminuserid
                  where au.adminusergroup=10 and date(d.createtime) = '%s' and d.itemtype=1
                  group by au.adminuserid,u.username;""",
               """select au.adminuserid,ur.username,count(u.messageid) from haodou_center.UserMessage u
                   inner join haodou_admin.AdminUser au on au.adminuserid=u.userid
                   left outer join haodou_passport.User ur on ur.userid=au.adminuserid
                   where au.adminusergroup=10 and date(u.updatetime) = '%s'
                   group by au.adminuserid,ur.username;
                   """,
             """select au.adminuserid,u.username,c.platform,c.type,count(c.commentid)
                from haodou_comment.Comment c
                inner join haodou_admin.AdminUser au on au.adminuserid=c.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where au.adminusergroup=10 and date(c.createtime) = '%s' and c.status=1
                group by au.adminuserid,u.username,c.platform,c.type;
                """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                 from haodou_admin.AdminLog log
                 inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                 left outer join haodou_passport.User u on u.userid=au.adminuserid
                 where date(log.createtime) = '%s' and au.adminusergroup=10
                 and log.menukey='recipe_recipe_cpsh' and log.logtype in(1,2,4,7)
                 group by au.adminuserid,u.username,log.logtype;
                 """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                from haodou_admin.AdminLog log
                inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where date(log.createtime) = '%s'
                and log.menukey='recipe_album_album' and log.logtype in(1,2,3) and au.adminusergroup=10
                group by au.adminuserid,u.username,log.logtype;
                """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                from haodou_admin.AdminLog log
                inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where date(log.createtime) = '%s'
                and log.menukey='recipe_recipe_photo' and log.logtype in(2,3) and au.adminusergroup=10
                group by au.adminuserid,u.username,log.logtype;
                """]

        # 查询的数据需要回写的SQL语句
        isqls=["""INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,-1,-1,1)""",
            """INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,-1,-1,2)""",
            """INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,%s,%s,3)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,1)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,2)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,3)"""]


        cnt=range(len(qsqls))

        for i in cnt:
           tjcur.execute(qsqls[i]  % rundate)
           result = []
           for row in tjcur.fetchall():
               result.append(tdate + row)
           bjcur.executemany(isqls[i],result)

        # close connection
        bjmysql.commit()
        bjmysql.close()
        tjmysql.commit()
        tjmysql.close()


if __name__ == '__main__':

    if len(sys.argv) < 2:
        rundate = str(date.fromordinal(date.today().toordinal() -1 ))
    else:
        rundate = sys.argv[1]

    print u"当前分析时间是：" + rundate
    adminAnalysis(rundate)
