package org.abigballofmud.datax.hook.model;


import lombok.*;

/**
 * <p>
 * datax数据同步统计类
 * </p>
 *
 * @author isacc 2020/5/13 17:28
 * @since 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class StatisticsDTO {

    public static final String INSERT_SQL = "INSERT INTO xdtx_statistics(exec_id, json_file_name, job_name, reader_plugin, writer_plugin, start_time, end_time, total_costs, byte_speed_per_second, record_speed_per_second, total_read_records, total_error_records, job_path, job_content, dirty_records)" +
            " values(?, ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?)";

    public static final int[] ARG_TYPES = new int[]{4, 12, 12, 12, 12, 12, 12, 12, 12, 12, -5, -5, 12, -1, -1};

    private Long id;

    /**
     * azkaban执行id
     */
    private String execId;
    /**
     * datax执行的json文件名
     */
    private String jsonFileName;
    /**
     * job名称
     */
    private String jobName;
    /**
     * reader插件名称
     */
    private String readerPlugin;
    /**
     * writer插件名称
     */
    private String writerPlugin;
    /**
     * 任务启动时刻
     */
    private String startTime;
    /**
     * 任务结束时刻
     */
    private String endTime;
    /**
     * 任务总计耗时，单位s
     */
    private String totalCosts;
    /**
     * 任务平均流量
     */
    private String byteSpeedPerSecond;
    /**
     * 记录写入速度
     */
    private String recordSpeedPerSecond;
    /**
     * 读出记录总数
     */
    private Long totalReadRecords;
    /**
     * 读写失败总数
     */
    private Long totalErrorRecords;
    /**
     * datax执行的json路径
     */
    private String jobPath;
    /**
     * datax的json内容
     */
    private String jobContent;
    /**
     * 脏数据即未同步成功的数据
     */
    private String dirtyRecords;

}
