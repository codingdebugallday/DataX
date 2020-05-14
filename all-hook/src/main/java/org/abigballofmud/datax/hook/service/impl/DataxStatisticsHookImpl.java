package org.abigballofmud.datax.hook.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.statistics.JobStatistics;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.abigballofmud.datax.hook.model.StatisticsDTO;
import org.abigballofmud.datax.hook.utils.DataSourceUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * 将datax任务统计信息写入mysql
 * </p>
 *
 * @author isacc 2020/5/13 17:36
 * @since 1.0
 */
public class DataxStatisticsHookImpl implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(DataxStatisticsHookImpl.class);

    @Override
    public String getName() {
        return "abigballofmud store datax statistics hook";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg, JobStatistics jobStatistics) {
        LOG.info("into abigballofmud store datax statistics hook");
        LOG.info("jobStatistics: {}", jobStatistics);
        LOG.info("load statistics.properties, path: {}", CoreConstant.DATAX_CONF_STATISTICS_PATH);
        handle(jobStatistics);
    }

    private void handle(JobStatistics jobStatistics) {
        JdbcTemplate jdbcTemplate = DataSourceUtil.getJdbcTemplate();
        int update = jdbcTemplate.update(StatisticsDTO.INSERT_SQL,
                genObjectArgArr(jobStatistics),
                StatisticsDTO.ARG_TYPES);
        if (update > 0) {
            LOG.info("jobStatistics insert into table success");
        } else {
            LOG.error("store to db error");
        }
    }

    private Object[] genObjectArgArr(JobStatistics jobStatistics) {
        List<Map<String, Object>> dirtyList = genDirtyList(jobStatistics);
        return new Object[]{
                jobStatistics.getExecId(),
                jobStatistics.getJsonFileName(),
                jobStatistics.getJobName(),
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
                jobStatistics.getJobContent(),
                CollectionUtils.isEmpty(dirtyList) ? null : JSON.toJSONString(dirtyList)
        };
    }

    private List<Map<String, Object>> genDirtyList(JobStatistics jobStatistics) {
        List<Pair<Record, String>> dirtyRecordList = jobStatistics.getDirtyRecordList();
        List<Map<String, Object>> list;
        if (dirtyRecordList.isEmpty()) {
            return Collections.emptyList();
        }
        // 暂时这样
        list = new ArrayList<>(dirtyRecordList.size());
        Map<String, Object> map;
        Map<String, Object> tmp;
        for (Pair<Record, String> pair : dirtyRecordList) {
            map = Maps.newHashMapWithExpectedSize(dirtyRecordList.size());
            map.put("errorMessage", pair.getRight().replace("'", "\\\""));
            List<Column> columnList = pair.getLeft().getColumnList();
            for (int i = 0, size = columnList.size(); i < size; i++) {
                tmp = Maps.newHashMapWithExpectedSize(size);
                tmp.put("rowData", columnList.get(i).getRawData());
                tmp.put("byteSize", columnList.get(i).getByteSize());
                tmp.put("type", columnList.get(i).getType());
                map.put("col" + i, tmp);
            }
            list.add(map);
        }
        return list;
    }

}
