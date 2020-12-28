package com.github.thestyleofme.datax.hook.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.github.thestyleofme.datax.hook.autoconfiguration.HookJpaConfiguration;
import com.github.thestyleofme.datax.hook.enums.HookErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * <p>
 * HooUtil
 * </p>
 *
 * @author isacc 2020/5/13 17:58
 * @since 1.0
 */
public class HookUtil {

    private HookUtil() {
        throw new IllegalStateException("util class");
    }

    private static final Logger LOG = LoggerFactory.getLogger(HookUtil.class);

    private static final ApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(HookJpaConfiguration.class);
    }

    public static Properties loadStatisticProperties() {
        // 单独测试时请在此设置datax.home
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(CoreConstant.DATAX_CONF_HOOK_PATH)))) {
            Properties properties = new Properties();
            properties.load(bufferedReader);
            return properties;
        } catch (IOException e) {
            LOG.error("statistic error", e);
            throw DataXException.asDataXException(HookErrorCode.LOAD_STATISTICS_PROPERTIES_ERROR,
                    "加载hook.properties出错.");
        }
    }

    public static ApplicationContext getContext() {
        return CONTEXT;
    }

    public static <T> T getBean(Class<T> clazz) {
        return CONTEXT.getBean(clazz);
    }

}
