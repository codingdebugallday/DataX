# DataX Hook
在DataX执行同步过后执行的逻辑，即com.alibaba.datax.common.spi.Hook接口的实现类逻辑，
这里主要是对本次DataX任务监控信息进行处理以及脏数据处理。

## 说明
项目使用azkaban进行datax的调度，需要对datax job执行情况进行监控，所以修改源码后对日志的一些监控信息进行存表以待后续运维。
> #### 1. 创表`src/main/conf/xdtx_statistics.sql`

> #### 2. 修改`src/main/conf/statistics.properties`里面的jdbcUrl等信息为自己的即可。

> #### 3. 开发了azkaban的datax插件去调度运行datax job，见[azkaban datax插件](https://github.com/thestyleofme/azkaban/tree/master/az-datax-jobtype-plugin/README.md)，当然不用azkaban的插件也可进行监控信息存储。

#### 联系：qq: 283273332
