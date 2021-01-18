package com.alibaba.datax.app.client.handler;

import com.alibaba.datax.app.client.RequestMapping;
import com.alibaba.datax.app.client.UrlHandler;
import com.alibaba.datax.app.pojo.Result;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * <p>
 * 测试连接，也作为心跳检测的请求
 * </p>
 *
 * @author isaac 2020/12/16 10:25
 * @since 1.0.0
 */
@RequestMapping(value = "/index")
public class IndexUrlHandler implements UrlHandler {

    @SuppressWarnings("unchecked")
    @Override
    public Result<String> handle(FullHttpRequest request) {
        return Result.ok("successful access to datax server, HOST: " + request.headers().get("HOST"));
    }


}
