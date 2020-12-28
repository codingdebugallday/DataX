package com.github.thestyleofme.datax.hook.app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.springframework.data.domain.Example;

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
        // 只有使用接口方式的才会去记录日志 使用datax.py脚本执行的不记录日志
        if (dataxJobExecutor == null) {
            return;
        }
        DataxJobLog dataxJobLog = new DataxJobLog();
        DataxJobLogRepository dataxJobLogRepository = HookUtil.getBean(DataxJobLogRepository.class);
        try {
            BeanUtils.copyProperties(dataxJobExecutor, dataxJobLog);
            // 得考虑一种情况 由于是读取节点日志文件 若在该机器本地起了三个服务 ip一样只是端口不同
            // 这样的话 日志信息是一样 应该只写一行记录即可 node为多个节点，逗号分割
            Example<DataxJobLog> example = Example.of(DataxJobLog.builder()
                    .jobId(dataxJobExecutor.getJobId())
                    .ip(dataxJobExecutor.getIp())
                    .build());
            Optional<DataxJobLog> one = dataxJobLogRepository.findOne(example);
            // 已经存在的话 更新node信息
            if (one.isPresent()) {
                DataxJobLog temp = one.get();
                dataxJobLog.setId(temp.getId());
                Set<String> nodes = Stream.of(temp.getNode().split(",")).collect(Collectors.toSet());
                nodes.add(dataxJobExecutor.getNode());
                dataxJobLog.setNode(String.join(",", nodes));
            }
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
