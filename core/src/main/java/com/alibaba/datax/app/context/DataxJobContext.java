package com.alibaba.datax.app.context;

import com.alibaba.datax.app.pojo.DataxJobInfo;
import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * <p>
 * datax执行时环境信息
 * </p>
 *
 * @author thestyleofme 2020/12/23 17:17
 * @since 1.0.0
 */
public class DataxJobContext {

    private DataxJobContext() {
        throw new IllegalStateException("context class!");
    }

    private static final TransmittableThreadLocal<DataxJobInfo> DATAX_JOB_CONTEXT_THREAD_LOCAL =
            new TransmittableThreadLocal<>();


    public static DataxJobInfo current() {
        return DATAX_JOB_CONTEXT_THREAD_LOCAL.get();
    }

    public static void setLogCollector(DataxJobInfo dataxJobInfo) {
        DATAX_JOB_CONTEXT_THREAD_LOCAL.set(dataxJobInfo);
    }

    public static void clear() {
        DATAX_JOB_CONTEXT_THREAD_LOCAL.remove();
    }

}
