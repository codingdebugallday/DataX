package org.abigballofmud.datax.hook.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.abigballofmud.datax.hook.enums.HookErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * <p>
 * 封装JdbcTemplate以及HikariDataSource
 * </p>
 *
 * @author isacc 2020/5/13 17:58
 * @since 1.0
 */
public class DataSourceUtil {

    private DataSourceUtil() {
        throw new IllegalStateException("util class");
    }

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceUtil.class);

    private static volatile HikariDataSource hikariDataSource;

    private static volatile JdbcTemplate jdbcTemplate;

    public static JdbcTemplate getJdbcTemplate() {
        if (Objects.isNull(jdbcTemplate)) {
            synchronized (DataSourceUtil.class) {
                if (Objects.isNull(jdbcTemplate)) {
                    return new JdbcTemplate(getDatasource());
                }
            }
        }
        return jdbcTemplate;
    }

    public static HikariDataSource getDatasource() {
        if (Objects.isNull(hikariDataSource)) {
            synchronized (DataSourceUtil.class) {
                if (Objects.isNull(hikariDataSource)) {
                    HikariConfig hikariConfig = new HikariConfig(loadStatisticProperties());
                    return new HikariDataSource(hikariConfig);
                }
            }
        }
        return hikariDataSource;
    }

    public static Properties loadStatisticProperties() {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(CoreConstant.DATAX_CONF_STATISTICS_PATH)))) {
            Properties properties = new Properties();
            properties.load(bufferedReader);
            return properties;
        } catch (IOException e) {
            LOG.error("statistic error", e);
            throw DataXException.asDataXException(HookErrorCode.LOAD_STATISTICS_PROPERTIES_ERROR,
                    "加载statistics.properties出错.");
        }
    }

}
