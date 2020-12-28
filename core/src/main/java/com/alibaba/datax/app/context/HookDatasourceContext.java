package com.alibaba.datax.app.context;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import javax.sql.DataSource;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * hook所用数据源储存，防止连接过多
 * </p>
 *
 * @author thestyleofme 2020/12/28 20:18
 * @since 1.0.0
 */
public class HookDatasourceContext {

    private HookDatasourceContext() {
        throw new IllegalStateException("context class!");
    }

    private static final Logger LOG = LoggerFactory.getLogger(HookDatasourceContext.class);
    private static final DataSource CACHE;

    static {
        HikariConfig hikariConfig = new HikariConfig(loadStatisticProperties());
        CACHE = new HikariDataSource(hikariConfig);
    }

    private static Properties loadStatisticProperties() {
        // 单独测试时请在此设置datax.home
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(CoreConstant.DATAX_CONF_HOOK_PATH)))) {
            Properties properties = new Properties();
            properties.load(bufferedReader);
            return properties;
        } catch (IOException e) {
            LOG.error("hook error");
            throw DataXException.asDataXException(FrameworkErrorCode.LOAD_HOOK_PROPERTIES_ERROR, "加载hook.properties出错.", e);
        }
    }

    public static DataSource getDatasource() {
        return CACHE;
    }
}
