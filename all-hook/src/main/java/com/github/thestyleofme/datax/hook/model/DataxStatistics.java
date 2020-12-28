package com.github.thestyleofme.datax.hook.model;

import javax.persistence.*;

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
@Entity
@Table(name = "datax_statistics")
public class DataxStatistics {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 执行id，其他调度平台的执行id
     */
    private Long execId;
    /**
     * datax的jobid
     */
    private Long jobId;
    /**
     * datax执行的json文件名
     */
    private String jsonFileName;
    /**
     * job名称
     */
    private String jobName;
    /**
     * datax执行的json路径
     */
    private String jobPath;
    /**
     * datax的json内容
     */
    private String jobContent;
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
     * 脏数据即未同步成功的数据
     */
    private String dirtyRecords;

}
