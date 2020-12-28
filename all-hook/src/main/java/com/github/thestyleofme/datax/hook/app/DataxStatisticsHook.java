package com.github.thestyleofme.datax.hook.app;

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
import com.github.thestyleofme.datax.hook.model.DataxStatistics;
import com.github.thestyleofme.datax.hook.repository.DataxStatisticsRepository;
import com.github.thestyleofme.datax.hook.utils.HookUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * 将datax任务统计信息写入mysql
 * </p>
 *
 * @author isacc 2020/5/13 17:36
 * @since 1.0
 */
public class DataxStatisticsHook implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(DataxStatisticsHook.class);

    @Override
    public String getName() {
        return "store datax statistics hook";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg, JobStatistics jobStatistics) {
        LOG.info("datax statistics hook...");
        LOG.info("load hook.properties, path: {}", CoreConstant.DATAX_CONF_HOOK_PATH);
        handle(jobStatistics);
    }

    private void handle(JobStatistics jobStatistics) {
        try {
            DataxStatisticsRepository dataxStatisticsRepository = HookUtil.getBean(DataxStatisticsRepository.class);
            DataxStatistics dataxStatistics = new DataxStatistics();
            BeanUtils.copyProperties(jobStatistics, dataxStatistics);
            // 脏数据格式转换
            List<Map<String, Object>> dirtyList = genDirtyList(jobStatistics);
            dataxStatistics.setDirtyRecords(CollectionUtils.isEmpty(dirtyList) ? null : JSON.toJSONString(dirtyList));
            dataxStatisticsRepository.save(dataxStatistics);
            LOG.info("datax job statistics insert into table success");
        } catch (Exception e) {
            LOG.error("datax job statistics insert error", e);
        }
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
            map.put("errorMessage", pair.getRight());
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
