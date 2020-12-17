package com.alibaba.datax.app.client;

import com.alibaba.datax.app.pojo.Result;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 11:31
 * @since 1.0.0
 */
public interface UrlHandler {

    /**
     * url handler
     *
     * @param request FullHttpRequest
     * @return String
     */
    <T> Result<T> handle(FullHttpRequest request);
}
