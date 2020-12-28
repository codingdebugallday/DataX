# DataX Hook
在DataX执行同步过后执行的逻辑，即com.alibaba.datax.common.spi.Hook接口的实现类逻辑，
这里主要是对本次DataX任务监控信息进行处理以及脏数据处理。

## 说明
对datax job执行情况进行监控，所以修改源码后对日志的一些监控信息进行存表以待后续运维。
> #### 1. 创表`src/main/conf/datax_hook.sql`

> #### 2. 修改`src/main/conf/hook.properties`里面的jdbcUrl等信息为自己的即可。

> #### 3. 打包，运行```mvn -U clean package assembly:assembly -pl all-hook -am -Dmaven.test.skip=true```