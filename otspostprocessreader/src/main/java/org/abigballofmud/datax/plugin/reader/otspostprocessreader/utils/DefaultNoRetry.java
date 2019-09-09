package org.abigballofmud.datax.plugin.reader.otspostprocessreader.utils;

import com.aliyun.openservices.ots.internal.OTSDefaultRetryStrategy;

public class DefaultNoRetry extends OTSDefaultRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }

}