package com.alibaba.datax.app.context;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * <p>
 * 暂时使用手动采集datax执行日志，接口请求返回日志信息
 * </p>
 *
 * @author isaac 2020/12/11 16:00
 * @since 1.0.0
 */
public class JobLogCollectorContext {

    private JobLogCollectorContext() {
        throw new IllegalStateException("context class!");
    }

    private static final TransmittableThreadLocal<StringBuilder> JOB_LOG_COLLECTOR_THREAD_LOCAL =
            new TransmittableThreadLocal<>();


    public static StringBuilder current() {
        return JOB_LOG_COLLECTOR_THREAD_LOCAL.get();
    }

    public static void setLogCollector(StringBuilder stringBuilder) {
        JOB_LOG_COLLECTOR_THREAD_LOCAL.set(stringBuilder);
    }

    public static void append(String log) {
        current().append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")))
                .append(" [").append(Thread.currentThread().getName()).append("]")
                .append(" - ")
                .append(log)
                .append("\n");
    }

    public static void clear() {
        JOB_LOG_COLLECTOR_THREAD_LOCAL.remove();
    }

}
