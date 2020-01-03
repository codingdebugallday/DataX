package com.alibaba.datax.common.statistics;

/**
 * <p>
 * DataX任务信息统计
 * </p>
 *
 * @author abigballofmud 2020/01/02 17:48
 * @since 1.0
 */
public class JobStatistics {

    private Long id;
    /**
     * datax执行的json文件名
     */
    private String jobPath;
    /**
     * datax执行的json文件名
     */
    private String jsonFileName;
    /**
     * azkaban执行ID
     */
    private Long execId;
    /**
     * DataX json字符串
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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getJobPath() {
        return jobPath;
    }

    public void setJobPath(String jobPath) {
        this.jobPath = jobPath;
    }

    public String getJsonFileName() {
        return jsonFileName;
    }

    public void setJsonFileName(String jsonFileName) {
        this.jsonFileName = jsonFileName;
    }

    public Long getExecId() {
        return execId;
    }

    public void setExecId(Long execId) {
        this.execId = execId;
    }

    public String getJobContent() {
        return jobContent;
    }

    public void setJobContent(String jobContent) {
        this.jobContent = jobContent;
    }

    public String getReaderPlugin() {
        return readerPlugin;
    }

    public void setReaderPlugin(String readerPlugin) {
        this.readerPlugin = readerPlugin;
    }

    public String getWriterPlugin() {
        return writerPlugin;
    }

    public void setWriterPlugin(String writerPlugin) {
        this.writerPlugin = writerPlugin;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getTotalCosts() {
        return totalCosts;
    }

    public void setTotalCosts(String totalCosts) {
        this.totalCosts = totalCosts;
    }

    public String getByteSpeedPerSecond() {
        return byteSpeedPerSecond;
    }

    public void setByteSpeedPerSecond(String byteSpeedPerSecond) {
        this.byteSpeedPerSecond = byteSpeedPerSecond;
    }

    public String getRecordSpeedPerSecond() {
        return recordSpeedPerSecond;
    }

    public void setRecordSpeedPerSecond(String recordSpeedPerSecond) {
        this.recordSpeedPerSecond = recordSpeedPerSecond;
    }

    public Long getTotalReadRecords() {
        return totalReadRecords;
    }

    public void setTotalReadRecords(Long totalReadRecords) {
        this.totalReadRecords = totalReadRecords;
    }

    public Long getTotalErrorRecords() {
        return totalErrorRecords;
    }

    public void setTotalErrorRecords(Long totalErrorRecords) {
        this.totalErrorRecords = totalErrorRecords;
    }

    @Override
    public String toString() {
        return "JobStatistics{" +
                "id=" + id +
                ", jobPath='" + jobPath + '\'' +
                ", jsonFileName='" + jsonFileName + '\'' +
                ", execId=" + execId +
                ", readerPlugin='" + readerPlugin + '\'' +
                ", writerPlugin='" + writerPlugin + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", totalCosts='" + totalCosts + '\'' +
                ", byteSpeedPerSecond='" + byteSpeedPerSecond + '\'' +
                ", recordSpeedPerSecond='" + recordSpeedPerSecond + '\'' +
                ", totalReadRecords=" + totalReadRecords +
                ", totalErrorRecords=" + totalErrorRecords +
                '}';
    }
}
