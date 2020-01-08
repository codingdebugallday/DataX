package com.alibaba.datax.common.statistics;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.ttl.TransmittableThreadLocal;
import org.apache.commons.lang3.tuple.Pair;

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

    private static final TransmittableThreadLocal<List<Pair<Record, String>>> DIRTY_RECORD_LIST_THREAD_LOCAL =
            new TransmittableThreadLocal<>();


    public static List<Pair<Record, String>> current() {
        return DIRTY_RECORD_LIST_THREAD_LOCAL.get();
    }

    public static void setDirtyRecordList(List<Pair<Record, String>> dirtyRecordList) {
        DIRTY_RECORD_LIST_THREAD_LOCAL.set(dirtyRecordList);
    }

    public static void clear() {
        DIRTY_RECORD_LIST_THREAD_LOCAL.remove();
    }

}
