package com.alibaba.datax.app.pojo;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/25 17:17
 * @since 1.0.0
 */
public class DataxJobExecutor {

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

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
}
