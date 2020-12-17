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

    private String mode = "standalone";
    private Long jobId = -1L;
    private String job;
    private String jobJson;

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
}
