/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.11.200_root
 Source Server Type    : MySQL
 Source Server Version : 50724
 Source Host           : 192.168.11.200:7233
 Source Schema         : hdsp_factory

 Target Server Type    : MySQL
 Target Server Version : 50724
 File Encoding         : 65001

 Date: 03/01/2020 15:57:57
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for xdtx_statistics
-- ----------------------------
DROP TABLE IF EXISTS `xdtx_statistics`;
CREATE TABLE `xdtx_statistics`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `exec_id` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'azkaban执行id',
  `json_file_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'datax执行的json文件名',
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
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
