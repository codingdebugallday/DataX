package com.alibaba.datax.common.statistics;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * <p>
 * description
 * </p>
 *
 * @author abigballofmud 2020-1-6 09:30:05
 * @since 1.0
 */
public class DirtyRecordContext {

    private DirtyRecordContext() {
        throw new IllegalStateException("context class!");
    }

    private static final TransmittableThreadLocal<List<Record>> DIRTY_RECORD_LIST_THREAD_LOCAL =
            new TransmittableThreadLocal<>();


    public static List<Record> current() {
        return DIRTY_RECORD_LIST_THREAD_LOCAL.get();
    }

    public static void setDirtyRecordList(List<Record> dirtyRecordList) {
        DIRTY_RECORD_LIST_THREAD_LOCAL.set(dirtyRecordList);
    }

    public static void clear() {
        DIRTY_RECORD_LIST_THREAD_LOCAL.remove();
    }

}
