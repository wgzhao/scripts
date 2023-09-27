# 说明

该仓库用于存储预发布和生产环境的调度、管理和监控等程序

## tuna.py

`tuna.py` 是给上游调度提供的统一任务执行脚本，对上游屏蔽各类任务执行的细节，对下提供各种执行任务的接入，脚本运行如下：

```shell
usage: tuna.py [-h]
               [-m {hive,presto,spark-sql,beeline,spark-submit,datax,shell,schema}]
               [-i INIT] -f FILENAME [-p PARAM [PARAM ...]] [-t TIMEOUT] [-c]
               [-u USER]

general purposal ETL schedule util.

optional arguments:
  -h, --help            show this help message and exit
  -m {hive,presto,spark-sql,beeline,spark-submit,datax,shell,schema}, --method {hive,presto,spark-sql,beeline,spark-submit,datax,shell,schema}
                        choose invoke method, datax is the default
  -i INIT, --init INIT  Initialization SQL file
  -f FILENAME, --file FILENAME
                        SQL from files
  -p PARAM [PARAM ...], --param PARAM [PARAM ...]
                        Variable substitution to apply to ETL engine
  -t TIMEOUT, --timeout TIMEOUT
                        max execution time(seconds) of a job0 means unlimited.
                        defaults to 72000,
  -c, --console         Only print message to console, not log to file
  -u USER, --user USER  user which running job
```

### 参数说明

- `-m` 任务执行方法，该参数仅接受 `hive,presto,spark-sql,beeline,spark-submit,datax,shell,schema`，其中的一个，如果不提供，则默认为 `datax`
- `-f` 实际执行的任务文件，依据执行方法不同，对文件的内容要求不同
- `-p` 该参数仅在 `-m` 参数为 `datax` 时有用，用来传递额外的控制参数给 `datax`，无特别要求，可不用
- `-t` 任务最长可运行时间，超过运行时间而未结束的，则会被终止，程序以非零值退出
- `-c` 日志打印在终端上
- `-u` 该参数仅限于 `-m` 参数为 `presto` 时有用，表示调用 `presto` 的运行用户可以不是默认值(hive)

`tuna` 脚本默认会写日志文件，文件目录为 `/opt/infalog/log` ，文件名的依次由下面几部分组成，最后由 `_` 合成:

- `tuna` 这是固定前缀，用来区分其他程序日志
- `<method>` 这是 `-m` 传递过来的值，用来具体执行的方法
- `<jobname>` 任务名称，这部分是通过分析 `-f` 传递过来的文件名，去掉文件后缀后获得
- `<timestamp>` 时间戳，任务运行的开始时间
- `<hostname>` 任务运行所在的服务器主机名
- `<pid>` 任务运行的进程ID
  
## 新接口说明

为了实现某些不可言说的目的，在 `fsbrowser_fastapi.py` 脚本中实现了一个灵活数据查询接口功能，访问方式如下：

```
/dataSelect?selectId=<select_id>&<other params>
```

该接口至少需要一个 `selectId` 参数，即查询ID号，以及其他可选参数，该接口相比之前 Java 开发四种接口的差别在于：

- 所有参数均为可选参数，没有传递的参数用空值表示，需要你的 SQL 脚本支持这种方式
- 查询源支持  `presto`, `allsql`, `clickhouse` ， 但不支持 `phoenix`
- 与交易相关的参数会自动替换，也就是 [EDW 项目文档](https://gitlab.lczq.com/grp_ds/edw/-/blob/master/README.md) 里提到的参数均会自动替换
- 接口支持结果下载，只需要额外传递 `_m=d` 即可，没有传递该参数或者传递为 `_m=display` 则表示为展示，和普通接口一致
- 默认情况下，如果接口下载，则下载的文件为csv格式，同时文件名同 `selectId` 参数一致，如果要指定文件名，则可以参数 `filename=` 参数
- 查询的 SQL 采取模板形式，比如 `select * from tbl where logdate=${TD}`, 而不是之前的 `select * from tbl where logdate=:TD`
>- 支持的参数范围可以参考SP计算，不支持两个特殊的`${NO}`和`${NOW}`，具体定义在参数2004中，参数值在`每天夜间19点`刷新
>- 由于数据源的采集时间不尽相同，为了避免夜间查询接口时，数据无法取到，增加数据源级别的参数，名称为`数据源编号+参数`
  >> TD：每天夜间19点刷新  
  >> UFTD：在UF采集完后刷新  
  >> CWTD：在CW采集完后刷新  
  ```sql
  --该脚本在夜间19点后至UF采集前都无法查询到数据
  select * from odsuf.allbranch where logdate='${TD}'
  --该脚本可以保证一直取到最新的数据
  select * from odsuf.allbranch where logdate='${UFTD}'
  ```

