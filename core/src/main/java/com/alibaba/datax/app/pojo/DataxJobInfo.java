package com.alibaba.datax.app.pojo;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 10:43
 * @since 1.0.0
 */
public class DataxJobInfo {

    /**
     * 如下是datax自带的，运行需要此三个参数
     */
    private String mode = "standalone";
    private Long jobId = -1L;
    private String job;
    /**
     * 如下是自定义
     */
    private String jobJson;
    private String jobName;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getJobJson() {
        return jobJson;
    }

    public void setJobJson(String jobJson) {
        this.jobJson = jobJson;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

}
