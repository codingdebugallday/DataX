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
@Table(name = "datax_job_log")
public class DataxJobLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    /**
     * datax的jobid
     */
    private Long jobId;
    /**
     * job名称
     */
    private String jobName;
    /**
     * 执行此job的datax节点的ip
     */
    private String ip;
    /**
     * 执行此job的datax节点
     */
    private String node;
    /**
     * 执行此job的log日志文件所在路径
     */
    private String logPath;
    /**
     * log具体内容
     */
    private String logContent;

}
