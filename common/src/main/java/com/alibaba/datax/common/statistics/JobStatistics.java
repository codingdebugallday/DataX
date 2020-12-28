package com.alibaba.datax.common.statistics;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.tuple.Pair;

/**
 * <p>
 * DataX任务信息统计
 * </p>
 *
 * @author thestyleofme 2020/01/02 17:48
 * @since 1.0
 */
public class JobStatistics {

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
     * datax执行的json文件名
     */
    private String jobPath;
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
    /**
     * 脏数据集合
     */
    private List<Pair<Record, String>> dirtyRecordList;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
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

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
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

    public List<Pair<Record, String>> getDirtyRecordList() {
        return dirtyRecordList;
    }

    public void setDirtyRecordList(List<Pair<Record, String>> dirtyRecordList) {
        this.dirtyRecordList = dirtyRecordList;
    }

    // ", dirtyRecordList.size()=" + (Objects.isNull(dirtyRecordList) ? 0 : dirtyRecordList.size()) +


    @Override
    public String toString() {
        return "JobStatistics{" +
                "id=" + id +
                ", execId=" + execId +
                ", jobId=" + jobId +
                ", jsonFileName='" + jsonFileName + '\'' +
                ", jobName='" + jobName + '\'' +
                ", jobPath='" + jobPath + '\'' +
                ", jobContent='" + jobContent + '\'' +
                ", readerPlugin='" + readerPlugin + '\'' +
                ", writerPlugin='" + writerPlugin + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", totalCosts='" + totalCosts + '\'' +
                ", byteSpeedPerSecond='" + byteSpeedPerSecond + '\'' +
                ", recordSpeedPerSecond='" + recordSpeedPerSecond + '\'' +
                ", totalReadRecords=" + totalReadRecords +
                ", totalErrorRecords=" + totalErrorRecords +
                ", dirtyRecordList=" + dirtyRecordList +
                '}';
    }
}
