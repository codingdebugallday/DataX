# DataX Hook
在DataX执行同步过后执行的逻辑，即com.alibaba.datax.common.spi.Hook接口的实现类逻辑，
这里主要是对本次DataX任务监控信息进行处理以及脏数据处理。

## 说明
项目使用azkaban进行datax的调度，需要对datax job执行情况进行监控，所以修改源码后对日志的一些监控信息进行存表以待后续运维。
> #### 1. 创表
```sql
-- ----------------------------
-- Table structure for xdtx_statistics
-- ----------------------------
DROP TABLE IF EXISTS `xdtx_statistics`;
CREATE TABLE `xdtx_statistics`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `exec_id` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'azkaban执行id',
  `json_file_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'datax执行的json文件名',
  `job_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'job名称',
  `reader_plugin` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'reader插件名称',
  `writer_plugin` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'writer插件名称',
  `start_time` varchar(63) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务启动时刻',
  `end_time` varchar(63) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务结束时刻',
  `total_costs` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务总计耗时，单位s',
  `byte_speed_per_second` varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务平均流量',
  `record_speed_per_second` varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '记录写入速度',
  `total_read_records` bigint(20) NULL DEFAULT NULL COMMENT '读出记录总数',
  `total_error_records` bigint(20) NULL DEFAULT NULL COMMENT '读写失败总数',
  `job_path` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'datax执行的json路径',
  `job_content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL COMMENT 'datax的json内容',
  `dirty_records` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL COMMENT '脏数据即未同步成功的数据',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 12 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;
```
> #### 2. 修改`org/abigballofmud/datax/hook/service/app/impl/StoreDataxStatisticsHookImpl.java`里面的jdbcUrl等信息为自己的即可。

> #### 3. 开发了azkaban的datax插件去调度运行datax job，见[azkaban datax插件](https://github.com/codingdebugallday/azkaban/tree/master/az-datax-jobtype-plugin/README.md)，当然不用azkaban的插件也可进行监控信息存储。

#### 联系：qq: 283273332
