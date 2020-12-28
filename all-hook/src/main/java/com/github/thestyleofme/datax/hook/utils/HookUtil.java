package com.github.thestyleofme.datax.hook.utils;

import com.github.thestyleofme.datax.hook.autoconfiguration.HookJpaConfiguration;
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

    private static final ApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(HookJpaConfiguration.class);
    }

    public static ApplicationContext getContext() {
        return CONTEXT;
    }

    public static <T> T getBean(Class<T> clazz) {
        return CONTEXT.getBean(clazz);
    }

}
