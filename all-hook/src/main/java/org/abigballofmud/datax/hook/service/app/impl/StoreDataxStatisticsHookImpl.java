package org.abigballofmud.datax.hook.service.app.impl;

import java.sql.*;
import java.util.Map;

import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.statistics.JobStatistics;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * description
 * </p>
 *
 * @author abigballofmud 2020/01/02 15:41
 * @since 1.0
 */
public class StoreDataxStatisticsHookImpl implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(StoreDataxStatisticsHookImpl.class);

    @Override
    public String getName() {
        return "abigballofmud store datax statistics hook";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg, JobStatistics jobStatistics) {
        // ignore
        LOG.info("into abigballofmud store datax statistics hook");
        LOG.info("jobStatistics: {}", jobStatistics);
        String insertSql = generateInsertSql(jobStatistics);
        storeToDb(insertSql);
    }

    private String generateInsertSql(JobStatistics jobStatistics) {
        String tableName = "xdtx_statistics";
        String columns = String.join(", ",
                "exec_id", "json_file_name", "reader_plugin", "writer_plugin",
                "start_time", "end_time", "total_costs", "byte_speed_per_second", "record_speed_per_second",
                "total_read_records", "total_error_records", "job_path", "job_content");
        return String.format("INSERT INTO %s(%s) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s')",
                tableName,
                columns,
                jobStatistics.getExecId(),
                jobStatistics.getJsonFileName(),
                jobStatistics.getReaderPlugin(),
                jobStatistics.getWriterPlugin(),
                jobStatistics.getStartTime(),
                jobStatistics.getEndTime(),
                jobStatistics.getTotalCosts(),
                jobStatistics.getByteSpeedPerSecond(),
                jobStatistics.getRecordSpeedPerSecond(),
                jobStatistics.getTotalReadRecords(),
                jobStatistics.getTotalErrorRecords(),
                jobStatistics.getJobPath(),
                jobStatistics.getJobContent()
        );
    }

    private void storeToDb(String insertSql) {
        String jdbcUrl = "jdbc:mysql://dev.hdsp.hand.com:7233/hdsp_factory?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowMultiQueries=true";
        String username = "hdsp_dev";
        String password = "hdsp_dev";
        try ( Connection connection = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
              Statement statement =  connection.createStatement()) {
            DBUtil.executeSqlWithoutResultSet(statement,insertSql);
            LOG.info("jobStatistics insert into table success");
        } catch (SQLException e) {
            LOG.error("store to db error", e);
        }
    }
}
