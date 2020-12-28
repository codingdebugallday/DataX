package com.github.thestyleofme.datax.hook.app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import com.alibaba.datax.app.context.DataxJobContext;
import com.alibaba.datax.app.pojo.DataxJobExecutor;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.statistics.JobStatistics;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.github.thestyleofme.datax.hook.model.DataxJobLog;
import com.github.thestyleofme.datax.hook.repository.DataxJobLogRepository;
import com.github.thestyleofme.datax.hook.utils.HookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

/**
 * <p>
 * datax job log插表记录
 * </p>
 *
 * @author thestyleofme 2020/12/25 14:42
 * @since 1.0.0
 */
public class DataxJobLogHook implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(DataxJobLogHook.class);

    @Override
    public String getName() {
        return "datax job log hook";
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg, JobStatistics jobStatistics) {
        LOG.info("datax job log hook...");
        LOG.info("load hook.properties, path: {}", CoreConstant.DATAX_CONF_HOOK_PATH);
        handle();
    }

    private void handle() {
        DataxJobExecutor dataxJobExecutor = DataxJobContext.current();
        DataxJobLog dataxJobLog = new DataxJobLog();
        DataxJobLogRepository dataxJobLogRepository = HookUtil.getBean(DataxJobLogRepository.class);
        try {
            BeanUtils.copyProperties(dataxJobExecutor, dataxJobLog);
            dataxJobLogRepository.save(dataxJobLog);
            LOG.info("datax job log insert into table success");
        } catch (Exception e) {
            LOG.error("datax job log insert error", e);
        } finally {
            DataxJobContext.clear();
            // save两次是为了更新log信息 因为第一次save日志还没打完
            save(dataxJobLogRepository, dataxJobLog, dataxJobLog.getLogPath());
        }
    }

    private void save(DataxJobLogRepository dataxJobLogRepository, DataxJobLog dataxJobLog, String path) {
        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path)))) {
            dataxJobLog.setLogContent(bufferedReader.lines().collect(Collectors.joining("\n")));
            dataxJobLogRepository.save(dataxJobLog);
        } catch (Exception e) {
            LOG.error("datax job log save error", e);
        }
    }

}
